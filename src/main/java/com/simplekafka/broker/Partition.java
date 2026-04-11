package com.simplekafka.broker;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class Partition {

    private final File logFile;
    private final AtomicLong nextOffset = new AtomicLong(0);

    public Partition(String topic, int partitionId) {
        File dir = new File("data" + File.separator + topic);
        if (!dir.exists()) {
            dir.mkdirs();
        }

        this.logFile = new File(dir, partitionId + ".log");
        initializeOffset();
    }

    private void initializeOffset() {
        if (!logFile.exists()) {
            return;
        }

        try (DataInputStream dis = new DataInputStream(new FileInputStream(logFile))) {
            long currentOffset = 0;
            while (dis.available() > 0) {
                int length = dis.readInt();
                dis.skipBytes(length);
                currentOffset++;
            }
            nextOffset.set(currentOffset);
        } catch (IOException ignored) {
        }
    }

    public synchronized long append(byte[] message) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(logFile, true)) {
            fos.write(intToBytes(message.length));
            fos.write(message);
            fos.flush();
        }
        return nextOffset.getAndIncrement();
    }

    public synchronized long append(byte[] message, long expectedOffset) throws IOException {
        if (expectedOffset != nextOffset.get()) {
            throw new IOException("Offset mismatch: expected=" + nextOffset.get() + " received=" + expectedOffset);
        }
        return append(message);
    }

    public synchronized List<byte[]> readFromOffset(long startOffset, int maxMessages) throws IOException {
        List<byte[]> result = new ArrayList<>();

        if (!logFile.exists()) {
            return result;
        }

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

    public long getNextOffset() {
        return nextOffset.get();
    }

    private byte[] intToBytes(int value) {
        return new byte[]{
                (byte) (value >> 24),
                (byte) (value >> 16),
                (byte) (value >> 8),
                (byte) value
        };
    }
}