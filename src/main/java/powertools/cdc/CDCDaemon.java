package org.apache.cassandra.cdc;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.collect.Sets;

import com.datastax.driver.core.Cluster;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.commitlog.CommitLogDescriptor;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.commitlog.CommitLogReadHandler;
import org.apache.cassandra.db.commitlog.CommitLogReader;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

import static com.codahale.metrics.MetricRegistry.name;

public class CDCDaemon
{
    private static MetricRegistry metrics = new MetricRegistry();
    private static final Timer segmentsRead = metrics.timer(name(CDCDaemon.class, "segments"));
    private static final Timer mutationsRead = metrics.timer(name(CDCDaemon.class, "mutations"));
    private static final Counter nonCDCMutation = metrics.counter("non-cdc-mutation");
    private static final Counter cdcDeletes = metrics.counter("cdc-deletes");
    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
    private final Path commitlogDirectory = Paths.get(System.getProperty("cassandra.commitlog"));
    private final Path cdcRawDirectory = Paths.get(System.getProperty("cassandra.cdc_raw"));
    private final Set<TableId> unknownCfids = Sets.newConcurrentHashSet();

    private TCPServer.QueueWriterAdapter outputQueue;
    private CDCHandler handler ;

    private CDCDaemon()
    {
        Config.setClientMode(true);
    }

    public static void main(String[] args)
    {
        new CDCDaemon().start();
    }

    private void tryRead(Path p, boolean canDelete, boolean canReload, boolean isFlushed)
    {
        final Timer.Context context = segmentsRead.time();
        try
        {
            CommitLogReader reader = new CommitLogReader();
            CommitLogDescriptor descriptor = CommitLogDescriptor.fromFileName(p.toFile().getName());
            reader.readCommitLogSegment(handler, p.toFile(), handler.getPosition(descriptor.id), CommitLogReader.ALL_MUTATIONS, false);
            if (reader.getInvalidMutations().isEmpty() && canDelete && isFlushed)
            {
                Files.delete(p);
                cdcDeletes.inc();
            }
            else
            {
                for (Map.Entry<TableId, AtomicInteger> entry : reader.getInvalidMutations())
                {
                    boolean newCfid = !unknownCfids.contains(entry.getKey());
                    if (canReload && newCfid)
                    {
                        reloadSchema();
                        tryRead(p, canDelete, false, isFlushed);
                    }
                    else if (newCfid)
                    {
                        System.err.println("Unknown cfid: " + entry.getKey() + " value: " + entry.getValue());
                        unknownCfids.add(entry.getKey());
                    }
                }
            }
        }
        catch (RuntimeException e)
        {
            if (e.getCause() instanceof NoSuchFileException)
            {
                // This is a race between when we list the files and when we actually try to read them.
                // If we are in the commitlog directory, this is an expected condition when we are finishing up the processing the file.
                // This occurs outside of the try-catch that is supposed to catch this
            }
            else
            {
                throw e;
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        catch (Exception e)
        {
            System.err.println("exception! " + e.getMessage());
        }
        finally{
            context.stop();
        }
    }

    private void readFolder(Path directory, boolean canDelete)
    {
        try
        {
            List<Future<?>> futures = new ArrayList<>();
            Stream<Path> files = Files.list(directory);

            files.forEach(p -> {
                futures.add(executor.submit(() -> {
                    try {
                        Stream<Path> unFlushedFiles = Files.list(commitlogDirectory);
                        boolean isFlushed = unFlushedFiles.filter(x -> x.getFileName().toString().equals(p.getFileName().toString())).count() == 0;
                        //p.getFileName().toString().equals(currentFile);
                        tryRead(p, canDelete, true, isFlushed);
                    }catch(Exception e){
                        e.printStackTrace();
                    }
                }));
            });
            for (Future<?> future : futures)
            {
                future.get();
            }
        }
        catch (IOException | InterruptedException | ExecutionException e)
        {
            e.printStackTrace(System.err);
        }
    }

    private void readRaw()
    {
        readFolder(cdcRawDirectory, true);
    }

    private void readCurrent()
    {
        readFolder(commitlogDirectory, false);
    }

    private void iteration()
    {
        Future<?> overflow = executor.submit(this::readRaw);
        //Future<?> current = executor.submit(this::readCurrent);
        try
        {
            overflow.get();
            //current.get();
        }
        catch (InterruptedException | ExecutionException e)
        {
            e.printStackTrace(System.err);
        }
    }

    private synchronized void reloadSchema()
    {
        for (String keyspaceName : Schema.instance.getKeyspaces())
        {
            for (TableMetadata cfm : Schema.instance.getTablesAndViews(keyspaceName))
            {
                Schema.instance.unsafeUnload(cfm);
            }
        }
        Cluster cluster = Cluster.builder()
                .addContactPoint("localhost")
                .withoutJMXReporting()
                .build();

        new RemoteSchema(cluster).load();
    }

    public void start()
    {
        final ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter.start(1, TimeUnit.MINUTES);

        DatabaseDescriptor.toolInitialization();
        // Since CDC is only in newer Cassandras, we know how to read the schema values from it
        reloadSchema();

        FileWatcher fw = new FileWatcher(commitlogDirectory, cdcRawDirectory);
        fw.watchAndHardlink();

        TCPServer tcpServer = new TCPServer();
        outputQueue = tcpServer.createTCPServer();
        handler = new SimpleCount(outputQueue);

        executor.scheduleAtFixedRate(this::iteration, 0, 250, TimeUnit.MILLISECONDS);
    }

    private interface CDCHandler extends CommitLogReadHandler
    {
        CommitLogPosition getPosition(long identifier);
    }

    private static class SimpleCount implements CDCHandler
    {
        private final Map<Long, Integer> furthestPosition = new HashMap<>();
        private final TCPServer.QueueWriterAdapter outputQueue;

        public SimpleCount(TCPServer.QueueWriterAdapter outputQueue) {
            this.outputQueue = outputQueue;
        }

        @Override
        public boolean shouldSkipSegmentOnError(CommitLogReadException exception) throws IOException
        {
            exception.printStackTrace(System.err);
            return false;
        }

        @Override
        public void handleUnrecoverableError(CommitLogReadException exception) {
            exception.printStackTrace(System.err);
        }

        @Override
        public void handleMutation(Mutation m, int size, int entryLocation, CommitLogDescriptor desc)
        {
            final Timer.Context context = mutationsRead.time();
            try{
                if (furthestPosition.getOrDefault(desc.id, 0) < entryLocation)
                {
                    boolean cdc = false;
                    for (TableId cfId : m.getTableIds())
                    {
                        if (Schema.instance.getTableMetadata(cfId).params.cdc)
                            cdc = true;
                    }

                    if (cdc) {
                        //System.out.println("Reading mutation " + m.toString(true));
                        for (PartitionUpdate partitionUpdate : m.getPartitionUpdates()) {
                            //System.out.println(String.format("\tKey %s Contains partition update %s", m.key(), partitionUpdate.toString()));
                            outputQueue.write(partitionUpdate.toString());
                        }
                    }else{
                        nonCDCMutation.inc();
                    }

                    furthestPosition.put(desc.id, entryLocation);
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally{
                context.close();
            }
        }

        @Override
        public CommitLogPosition getPosition(long identifier)
        {
            return Optional.ofNullable(furthestPosition.get(identifier))
                    .map(i -> new CommitLogPosition(identifier, i))
                    .orElse(CommitLogPosition.NONE);
        }
    }
}
