package com.simplekafka.broker;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterManager {

    private static final Set<Integer> activeBrokers =
            ConcurrentHashMap.newKeySet();

    private static final Map<Integer, Integer> partitionLeaders =
            new ConcurrentHashMap<>();

    // Register broker
    public static void registerBroker(int brokerId) {
        activeBrokers.add(brokerId);
        System.out.println("Broker registered: " + brokerId);
    }

    // Remove broker (simulate failure)
    public static void removeBroker(int brokerId) {
        activeBrokers.remove(brokerId);
        System.out.println("Broker removed: " + brokerId);
        reElectAll();
    }

    // Get leader for partition
    public static int getLeader(int partitionId) {
        return partitionLeaders.getOrDefault(partitionId, -1);
    }

    // Assign initial leaders
    public static void assignLeaders(int partitions) {

        List<Integer> brokers = new ArrayList<>(activeBrokers);
        Collections.sort(brokers);

        if (brokers.isEmpty()) return;

        for (int i = 0; i < partitions; i++) {
            int leader = brokers.get(i % brokers.size());
            partitionLeaders.put(i, leader);
        }

        System.out.println("Leaders assigned: " + partitionLeaders);
    }

    // Re-election logic
    public static void reElectAll() {

        List<Integer> brokers = new ArrayList<>(activeBrokers);
        Collections.sort(brokers);

        if (brokers.isEmpty()) return;

        for (int partitionId : partitionLeaders.keySet()) {

            int currentLeader = partitionLeaders.get(partitionId);

            if (!brokers.contains(currentLeader)) {

                int newLeader = brokers.get(0);

                partitionLeaders.put(partitionId, newLeader);

                System.out.println(
                        "New leader for partition " +
                        partitionId + " -> " + newLeader
                );
            }
        }
    }

    public static Set<Integer> getActiveBrokers() {
        return activeBrokers;
    }
}