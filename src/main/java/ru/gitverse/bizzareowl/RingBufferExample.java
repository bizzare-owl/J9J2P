package ru.gitverse.bizzareowl;

import ru.gitverse.bizzareowl.ringbuffer.RingBuffer;

public class RingBufferExample {

    public static void main(String[] args) {
        RingBuffer<String> stringRingBuffer = new RingBuffer<>(3);
        stringRingBuffer.add("value1");
        stringRingBuffer.add("value2");
        System.out.println(stringRingBuffer);
        stringRingBuffer.add("value3");
        System.out.println(stringRingBuffer);
        stringRingBuffer.add("value4");
        System.out.println(stringRingBuffer);
        System.out.println(stringRingBuffer.get());
        System.out.println(stringRingBuffer.get());
        System.out.println(stringRingBuffer.get());
        System.out.println(stringRingBuffer);
        stringRingBuffer.add("value5");
        System.out.println(stringRingBuffer);
        System.out.println(stringRingBuffer.get());
        System.out.println(stringRingBuffer.get());
    }

}
