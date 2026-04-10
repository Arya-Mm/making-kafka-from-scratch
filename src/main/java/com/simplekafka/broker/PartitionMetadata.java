package com.simplekafka.broker;

import java.util.List;

public class PartitionMetadata {

    public final int leader;
    public final List<Integer> followers;

    public PartitionMetadata(int leader, List<Integer> followers) {
        this.leader = leader;
        this.followers = followers;
    }
}