package com.simplekafka.broker;

import java.util.HashMap;
import java.util.Map;

public class ConsumerGroupManager {

    // groupId → (partition → offset)
    private final Map<String, Map<Integer, Long>> groupOffsets = new HashMap<>();

    public synchronized long getOffset(String groupId, int partition) {
        groupOffsets.putIfAbsent(groupId, new HashMap<>());
        Map<Integer, Long> partitionOffsets = groupOffsets.get(groupId);

        return partitionOffsets.getOrDefault(partition, 0L);
    }

    public synchronized void commitOffset(String groupId, int partition, long offset) {
        groupOffsets.putIfAbsent(groupId, new HashMap<>());
        groupOffsets.get(groupId).put(partition, offset);
    }
}