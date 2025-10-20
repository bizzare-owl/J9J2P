package ru.gitverse.bizzareowl.ringbuffer;

import java.util.Arrays;

public class RingBuffer<T> {

    private final int size;
    private final Object[] buffer;
    private int readPointer = 0;
    private int writePointer = 0;
    private boolean overwriting = false;

    public RingBuffer(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Size of buffer cannot be less or equal to zero");
        }

        this.size = size;
        this.buffer = new Object[size];
    }

    public synchronized void add(T value) {
        if (overwriting && writePointer == readPointer) {
            readPointer = (readPointer + 1) % size;
        }

        buffer[writePointer] = value;
        writePointer = (writePointer + 1) % size;

        if (writePointer == readPointer) {
            overwriting = true;
        }
    }

    @SuppressWarnings("unchecked")
    public synchronized T get() {
        if (!overwriting && readPointer == writePointer) {
            throw new IllegalStateException("Attempt to read data from empty buffer");
        }

        T value = (T) buffer[readPointer];
        buffer[readPointer] = null;
        readPointer = (readPointer + 1) % size;

        if (overwriting && readPointer == writePointer) {
            overwriting = false;
        }

        return value;
    }

    @Override
    public String toString() {
        return Arrays.toString(buffer);
    }

}
