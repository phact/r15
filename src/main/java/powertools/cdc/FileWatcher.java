package powertools.cdc;

/*
 *
 * @author Sebastián Estévez on 11/30/18.
 *
 */

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.stream.Stream;

import static java.nio.file.StandardWatchEventKinds.*;

public class FileWatcher {

    private static WatchService watcher;
    private final Path commitlogDirectory;
    private final Path cdcRawDirectory;

    public FileWatcher(Path commitlogDirectory, Path cdcRawDirectory) {
        this.commitlogDirectory = commitlogDirectory;
        this.cdcRawDirectory = cdcRawDirectory;
    }

    public void watchAndHardlink(){

        //first hardlink existing commitlogs so we can start processing them
        try {
            Stream<Path> files = Files.list(commitlogDirectory);
            files.forEach(this::hardLink);
            files.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        //then watch the commitlog dir and continue to hardlink them as they come
        try {
            watcher = FileSystems.getDefault().newWatchService();
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            WatchKey key = commitlogDirectory.register(watcher,
                    ENTRY_CREATE
                    //,ENTRY_MODIFY
            );
        } catch (IOException x) {
            System.err.println(x);
        }

        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (;;) {
                    // wait for key to be signaled
                    WatchKey key;
                    try {
                        key = watcher.take();
                    } catch (InterruptedException x) {
                        return;
                    }

                    for (WatchEvent<?> event: key.pollEvents()) {
                        WatchEvent.Kind<?> kind = event.kind();

                        if (kind == OVERFLOW) {
                            continue;
                        }

                        WatchEvent<Path> ev = (WatchEvent<Path>)event;
                        Path filename = ev.context();

                        hardLink(filename);

                    }
                    boolean valid = key.reset();
                    if (!valid) {
                        System.out.println("Watch key is invalid, this should never happen.");
                        break;
                    }
                }
            }
        });
        thread.setDaemon(true);
        thread.start();
    }

    private void hardLink(Path filename){
        Path newLink = this.cdcRawDirectory.resolve(filename.getFileName());
        Path existingFile = this.commitlogDirectory.resolve(filename.getFileName());
        try {
            Files.createLink(newLink, existingFile);
        } catch (FileAlreadyExistsException x) {
        } catch (IOException x) {
            System.err.println(x);
        } catch (UnsupportedOperationException x) {
            System.err.println(x);
        }
    }
}
