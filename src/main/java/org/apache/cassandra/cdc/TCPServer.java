package org.apache.cassandra.cdc;

/*
 *
 * @author Sebastián Estévez on 12/4/18.
 *
 */


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class TCPServer {

    private ServerSocket listenerSocket;

    private final static Logger logger = LoggerFactory.getLogger(TCPServer.class);

    private BlockingQueue<String> queue = new LinkedBlockingQueue<>(10);
    private List<Shutdown> managedShutdown = new ArrayList<>();



    public QueueWriterAdapter createTCPServer(){
        String host = "localhost";
        int port = 12345;

        try {
            InetAddress hostAddr = InetAddress.getByName(host);
            ServerSocket listenerSocket = new ServerSocket(port,10, hostAddr);

            SocketAcceptor socketAcceptor = new SocketAcceptor(queue, listenerSocket);
            managedShutdown.add(socketAcceptor);
            Thread acceptorThread = new Thread(socketAcceptor);
            acceptorThread.setDaemon(true);
            acceptorThread.setName("Listener/" + listenerSocket);
            acceptorThread.start();
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Error listening on listenerSocket:" + e, e);
        }

        QueueWriterAdapter queueWriterAdapter = new QueueWriterAdapter(this.queue);

        logger.info("initialized queue writer:" + queueWriterAdapter);
        return queueWriterAdapter;

    }

    private static interface Shutdown {
        void shutdown();
    }

    public static class SocketWriter implements Runnable, Shutdown {
        private final BlockingQueue<String> sourceQueue;
        private final OutputStream outputStream;
        private final OutputStreamWriter writer;
        private boolean running = true;


        public SocketWriter(BlockingQueue<String> sourceQueue, Socket connectedSocket) {
            this.sourceQueue = sourceQueue;
            try {
                outputStream = connectedSocket.getOutputStream();
                this.writer = new OutputStreamWriter(outputStream);
                //connectedSocket.shutdownInput();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void shutdown() {
            this.running = false;
        }

        @Override
        public void run() {
            try (Writer writer = this.writer) {
                while (true) {
                    while (!sourceQueue.isEmpty() || running) {
                        try {
                            String data = sourceQueue.take();
                            writer.write(data);
                            writer.flush();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    }
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException ignored) {
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

        }

    }

    public static class QueueWriterAdapter extends Writer {
        private BlockingQueue<String> queue;

        public QueueWriterAdapter(BlockingQueue<String> queue) {
            this.queue = queue;
        }

        @Override
        public synchronized void write( char[] cbuf, int off, int len) {
            while (true) {
                try {
                    queue.put(new String(cbuf, off, len));
                    return;
                } catch (InterruptedException ignored) {
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public synchronized void flush() throws IOException {
        }

        @Override
        public synchronized void close() throws IOException {
            flush();
            queue = null;
        }

    }

    public class SocketAcceptor implements Runnable, Shutdown {
        private final BlockingQueue<String> queue;
        private final ServerSocket serverSocket;
        private boolean running = true;

        public SocketAcceptor(BlockingQueue<String> queue, ServerSocket serverSocket) {
            this.queue = queue;
            this.serverSocket = serverSocket;
        }

        public void shutdown() {
            this.running = false;
        }

        @Override
        public void run() {
            try (ServerSocket serverSocket = this.serverSocket) {
                while (running) {
                    serverSocket.setSoTimeout(1000);
                    serverSocket.setReuseAddress(true);
                    try {
                        Socket connectedSocket = serverSocket.accept();
                        SocketWriter writer = new SocketWriter(queue, connectedSocket);
                        //TCPServer.this.managedShutdown.add(writer);
                        Thread writerThread = new Thread(writer);
                        writerThread.setName("SocketWriter/" + connectedSocket);
                        writerThread.setDaemon(true);
                        writerThread.start();
                        logger.info("Started writer thread for " + connectedSocket);
                    } catch (SocketTimeoutException ignored) {
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

}
