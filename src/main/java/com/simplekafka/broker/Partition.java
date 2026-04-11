package com.simplekafka.broker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Partition {

    private static final int SEGMENT_MAX_SIZE = 1024 * 1024;
    private static final String LOG_SUFFIX = ".log";
    private static final String INDEX_SUFFIX = ".index";

    private final String topic;
    private final int partitionId;
    private final File partitionDir;
    private final AtomicLong nextOffset = new AtomicLong();
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final List<SegmentInfo> segments = new ArrayList<>();
    private SegmentInfo activeSegment;

    public Partition(String topic, int partitionId, String baseDir) throws IOException {
        this.topic = topic;
        this.partitionId = partitionId;
        this.partitionDir = new File(baseDir, topic + File.separator + partitionId);
        if (!partitionDir.exists() && !partitionDir.mkdirs()) {
            throw new IOException("Unable to create partition directory: " + partitionDir);
        }
        initialize();
    }

    private void initialize() throws IOException {
        File[] logFiles = partitionDir.listFiles((dir, name) -> name.endsWith(LOG_SUFFIX));
        if (logFiles != null) {
            for (File logFile : logFiles) {
                long baseOffset = parseBaseOffset(logFile.getName(), LOG_SUFFIX);
                File indexFile = new File(partitionDir, formatOffset(baseOffset) + INDEX_SUFFIX);
                SegmentInfo segment = new SegmentInfo(baseOffset, logFile, indexFile);
                segment.openForRead();
                segments.add(segment);
            }
        }

        segments.sort(Comparator.comparingLong(s -> s.baseOffset));

        if (segments.isEmpty()) {
            createNewSegment(0);
        } else {
            activeSegment = segments.get(segments.size() - 1);
            activeSegment.openForAppend();
            nextOffset.set(activeSegment.baseOffset + activeSegment.getMessageCount());
        }
    }

    public long append(byte[] message) throws IOException {
        lock.writeLock().lock();
        try {
            if (activeSegment.logChannel.size() >= SEGMENT_MAX_SIZE) {
                createNewSegment(nextOffset.get());
            }

            long offset = nextOffset.getAndIncrement();
            long position = activeSegment.logChannel.position();

            ByteBuffer buffer = ByteBuffer.allocate(4 + message.length);
            buffer.putInt(message.length).put(message).flip();
            while (buffer.hasRemaining()) {
                activeSegment.logChannel.write(buffer);
            }
            activeSegment.logChannel.force(true);
            activeSegment.appendIndex(offset, position);

            return offset;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public long append(byte[] message, long expectedOffset) throws IOException {
        if (expectedOffset != nextOffset.get()) {
            throw new IOException("Offset mismatch: expected=" + nextOffset.get() + " received=" + expectedOffset);
        }
        return append(message);
    }

    public List<byte[]> readFromOffset(long startOffset, int maxMessages) throws IOException {
        lock.readLock().lock();
        try {
            List<byte[]> result = new ArrayList<>();
            if (segments.isEmpty()) {
                return result;
            }

            int segmentIndex = findSegmentIndexForOffset(startOffset);
            if (segmentIndex < 0) {
                return result;
            }

            long nextOffsetToRead = startOffset;
            for (int i = segmentIndex; i < segments.size() && result.size() < maxMessages; i++) {
                SegmentInfo segment = segments.get(i);
                long position = findPositionForOffset(segment, nextOffsetToRead);
                if (position < 0) {
                    if (i == segmentIndex) {
                        return result;
                    }
                    position = 0;
                }

                try (RandomAccessFile raf = new RandomAccessFile(segment.logFile, "r");
                     FileChannel channel = raf.getChannel()) {

                    channel.position(position);
                    while (result.size() < maxMessages && channel.position() < channel.size()) {
                        ByteBuffer lenBuf = ByteBuffer.allocate(4);
                        if (channel.read(lenBuf) < 4) {
                            break;
                        }
                        lenBuf.flip();
                        int length = lenBuf.getInt();
                        ByteBuffer msgBuf = ByteBuffer.allocate(length);
                        if (channel.read(msgBuf) < length) {
                            break;
                        }
                        msgBuf.flip();
                        result.add(msgBuf.array());
                        nextOffsetToRead++;
                    }
                }
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    public long getNextOffset() {
        return nextOffset.get();
    }

    private void createNewSegment(long baseOffset) throws IOException {
        if (activeSegment != null) {
            activeSegment.close();
        }

        File logFile = new File(partitionDir, formatOffset(baseOffset) + LOG_SUFFIX);
        File indexFile = new File(partitionDir, formatOffset(baseOffset) + INDEX_SUFFIX);

        SegmentInfo segment = new SegmentInfo(baseOffset, logFile, indexFile);
        segment.createFiles();
        segment.openForAppend();

        segments.add(segment);
        activeSegment = segment;
    }

    private int findSegmentIndexForOffset(long offset) {
        for (int i = 0; i < segments.size(); i++) {
            SegmentInfo current = segments.get(i);
            long nextBase = (i + 1 < segments.size()) ? segments.get(i + 1).baseOffset : Long.MAX_VALUE;
            if (offset >= current.baseOffset && offset < nextBase) {
                return i;
            }
        }
        return -1;
    }

    private long findPositionForOffset(SegmentInfo segment, long offset) throws IOException {
        long relativeOffset = offset - segment.baseOffset;
        if (relativeOffset < 0 || relativeOffset >= segment.getMessageCount()) {
            return -1;
        }

        ByteBuffer buffer = ByteBuffer.allocate(16);
        segment.indexChannel.position(relativeOffset * 16);
        if (segment.indexChannel.read(buffer) < 16) {
            return -1;
        }
        buffer.flip();
        buffer.getLong();
        return buffer.getLong();
    }

    private long parseBaseOffset(String fileName, String suffix) {
        String raw = fileName.substring(0, fileName.length() - suffix.length());
        return Long.parseLong(raw);
    }

    private String formatOffset(long offset) {
        return String.format("%020d", offset);
    }

    private static class SegmentInfo {
        private final long baseOffset;
        private final File logFile;
        private final File indexFile;

        private RandomAccessFile logRaf;
        private FileChannel logChannel;

        private RandomAccessFile indexRaf;
        private FileChannel indexChannel;

        SegmentInfo(long baseOffset, File logFile, File indexFile) {
            this.baseOffset = baseOffset;
            this.logFile = logFile;
            this.indexFile = indexFile;
        }

        void createFiles() throws IOException {
            if (!logFile.exists() && !logFile.createNewFile()) {
                throw new IOException("Unable to create log file: " + logFile);
            }
            if (!indexFile.exists() && !indexFile.createNewFile()) {
                throw new IOException("Unable to create index file: " + indexFile);
            }
        }

        void openForRead() throws IOException {
            if (!logFile.exists()) {
                throw new IOException("Missing log file: " + logFile);
            }
            if (!indexFile.exists()) {
                indexFile.createNewFile();
            }

            this.indexRaf = new RandomAccessFile(indexFile, "rw");
            this.indexChannel = indexRaf.getChannel();
        }

        void openForAppend() throws IOException {
            createFiles();

            if (logRaf != null) {
                close();
            }

            this.logRaf = new RandomAccessFile(logFile, "rw");
            this.logChannel = logRaf.getChannel();
            this.logChannel.position(this.logChannel.size());

            this.indexRaf = new RandomAccessFile(indexFile, "rw");
            this.indexChannel = indexRaf.getChannel();
            this.indexChannel.position(this.indexChannel.size());
        }

        long getMessageCount() throws IOException {
            return indexChannel.size() / 16;
        }

        void appendIndex(long offset, long position) throws IOException {
            ByteBuffer buffer = ByteBuffer.allocate(16);
            buffer.putLong(offset).putLong(position).flip();
            while (buffer.hasRemaining()) {
                indexChannel.write(buffer);
            }
            indexChannel.force(true);
        }

        void close() throws IOException {
            if (logChannel != null) {
                logChannel.close();
            }
            if (logRaf != null) {
                logRaf.close();
            }
            if (indexChannel != null) {
                indexChannel.close();
            }
            if (indexRaf != null) {
                indexRaf.close();
            }
        }
    }
}