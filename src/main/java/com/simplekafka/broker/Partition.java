package com.simplekafka.broker;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Partition {

    private final File logFile;
    private final AtomicLong offset = new AtomicLong(0);

    public Partition(String topic) {
        File dir = new File("data");
        if (!dir.exists()) dir.mkdir();

        this.logFile = new File(dir, topic + ".log");
    }

    public synchronized long append(byte[] message) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(logFile, true)) {
            fos.write(intToBytes(message.length));
            fos.write(message);
        }
        return offset.getAndIncrement();
    }

    public synchronized List<byte[]> readFromOffset(long startOffset, int maxMessages) throws IOException {
        List<byte[]> result = new ArrayList<>();

        if (!logFile.exists()) return result;

        try (DataInputStream dis = new DataInputStream(new FileInputStream(logFile))) {

            long currentOffset = 0;

            while (dis.available() > 0 && result.size() < maxMessages) {

                int length = dis.readInt();
                byte[] message = new byte[length];
                dis.readFully(message);

                if (currentOffset >= startOffset) {
                    result.add(message);
                }

                currentOffset++;
            }
        }

        return result;
    }

    private byte[] intToBytes(int value) {
        return new byte[] {
                (byte)(value >> 24),
                (byte)(value >> 16),
                (byte)(value >> 8),
                (byte)value
        };
    }
}