package ru.gitverse.bizzareowl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TcpEchoServer {

    public static void main(String[] args) throws IOException {

        final ExecutorService executor = Executors.newFixedThreadPool(10);

        record EchoTask(SocketChannel socketChannel) implements Runnable {

            @Override
            public void run() {
                try (socketChannel) {
                    ByteBuffer byteBuffer = ByteBuffer.allocate(1 << 10);
                    while (socketChannel.isConnected()) {
                        int read = socketChannel.read(byteBuffer);

                        if (read == -1) {
                            break;
                        }

                        byteBuffer.flip();
                        socketChannel.write(byteBuffer);
                        byteBuffer.clear();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

        }

        try (ServerSocketChannel serverSocketChannel = ServerSocketChannel.open()) {

            serverSocketChannel.bind(new InetSocketAddress(7));
            while (true) {

                SocketChannel socketChannel = serverSocketChannel.accept();
                if (socketChannel == null) {
                    continue;
                }

                System.out.println("Accepted connection");
                executor.submit(new EchoTask(socketChannel));
            }
        } finally {
            executor.close();
        }
    }

}
