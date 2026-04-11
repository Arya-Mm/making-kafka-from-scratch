package com.simplekafka.broker;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionManager {

    private final Map<Integer, Partition> partitions = new ConcurrentHashMap<>();
    private final int numPartitions;

    public PartitionManager(String topic, int numPartitions) {
        this.numPartitions = numPartitions;

        for (int i = 0; i < numPartitions; i++) {
            partitions.put(i, new Partition(topic, i));
        }
    }

    public Partition getPartition(int partitionId) {
        return partitions.get(partitionId);
    }

    public int getPartitionCount() {
        return numPartitions;
    }

    public int getPartitionForKey(String key) {
        return Math.floorMod(key.hashCode(), numPartitions);
    }
}