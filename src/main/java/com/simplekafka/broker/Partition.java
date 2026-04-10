package com.simplekafka.broker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
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

            // Write message length
            fos.write(intToBytes(message.length));

            // Write message
            fos.write(message);
        }

        return offset.getAndIncrement();
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