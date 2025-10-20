package ru.gitverse.bizzareowl;

public class OddEvenThreads {

    public static void main(String[] args) throws InterruptedException {

        Thread odd = new Thread(() -> {
            for (int i = 1; i < 1000; i += 1) {
                System.out.println("Thread[Odd]:\t" + i);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Thread even = new Thread(() -> {
            for (int i = 2; i < 1000; i += 2) {
                System.out.println("Thread[Even]:\t" + i);
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        odd.start();
        even.start();
        odd.join();
        even.join();

    }

}
