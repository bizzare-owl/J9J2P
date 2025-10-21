package ru.gitverse.bizzareowl;

public class OddEvenThreads {

    private static class IntegerIterator {
        private int i = 1;

        public synchronized void increment() {
            i++;
            notify();
        }

        public synchronized int getOdd() throws InterruptedException {
            if (i % 2 == 0) {
                wait();
            }
            int odd = i;
            increment();
            return odd;
        }

        public synchronized int getEven() throws InterruptedException {
            if (i % 2 == 1) {
                wait();
            }
            int even = i;
            increment();
            return even;
        }
    }

    public static void main(String[] args) throws InterruptedException {

        IntegerIterator iterator = new IntegerIterator();

        Thread odd = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                try {
                    System.out.println("Thread[Odd]:\t" + iterator.getOdd());
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        Thread even = new Thread(() -> {
            for (int i = 0; i < 1000; i++) {
                try {
                    System.out.println("Thread[Even]:\t" + iterator.getEven());
                    Thread.sleep(1000);
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
