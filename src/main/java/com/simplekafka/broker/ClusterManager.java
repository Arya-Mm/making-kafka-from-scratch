package com.simplekafka.broker;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ClusterManager {

    // 🔥 ACTIVE BROKERS
    private static final Set<Integer> activeBrokers =
            ConcurrentHashMap.newKeySet();

    // 🔥 PARTITION LEADERS
    private static final Map<Integer, Integer> partitionLeaders =
            new ConcurrentHashMap<>();

    // 🔥 REGISTER BROKER
    public static void registerBroker(int brokerId) {
        activeBrokers.add(brokerId);
        System.out.println("Broker registered: " + brokerId);
    }

    // 🔥 REMOVE BROKER (simulate failure)
    public static void removeBroker(int brokerId) {
        activeBrokers.remove(brokerId);
        System.out.println("Broker removed: " + brokerId);

        // 🔥 trigger re-election
        reElectAll();
    }

    // 🔥 GET LEADER
    public static int getLeader(int partitionId) {
        return partitionLeaders.getOrDefault(partitionId, -1);
    }

    // 🔥 INITIAL ASSIGNMENT
    public static void assignLeaders(int partitions) {

        List<Integer> brokers = new ArrayList<>(activeBrokers);
        Collections.sort(brokers);

        for (int i = 0; i < partitions; i++) {
            int leader = brokers.get(i % brokers.size());
            partitionLeaders.put(i, leader);
        }

        System.out.println("Leaders assigned: " + partitionLeaders);
    }

    // 🔥 RE-ELECTION
    public static void reElectAll() {

        List<Integer> brokers = new ArrayList<>(activeBrokers);
        Collections.sort(brokers);

        for (int partitionId : partitionLeaders.keySet()) {

            if (!brokers.contains(partitionLeaders.get(partitionId))) {

                int newLeader = brokers.get(0); // simple strategy
                partitionLeaders.put(partitionId, newLeader);

                System.out.println(
                        "New leader for partition " +
                        partitionId + " → " + newLeader
                );
            }
        }
    }

    public static Set<Integer> getActiveBrokers() {
        return activeBrokers;
    }
}