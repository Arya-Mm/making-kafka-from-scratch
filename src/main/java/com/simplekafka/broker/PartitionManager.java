package com.simplekafka.broker;

import java.util.HashMap;
import java.util.Map;

public class PartitionManager {

    private final Map<Integer, Partition> partitions = new HashMap<>();
    private final int numPartitions;

    public PartitionManager(String topic, int numPartitions) {
        this.numPartitions = numPartitions;

        for (int i = 0; i < numPartitions; i++) {
            partitions.put(i, new Partition(topic + "-" + i));
        }
    }

    public Partition getPartition(int partitionId) {
        return partitions.get(partitionId);
    }

    public int getPartitionForKey(String key) {
        return Math.abs(key.hashCode()) % numPartitions;
    }
}