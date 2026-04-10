package com.simplekafka.broker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class ZookeeperClient implements Watcher {

    private ZooKeeper zooKeeper;
    private CountDownLatch connectedSignal = new CountDownLatch(1);

    private final String host;
    private final int port;

    private static final int SESSION_TIMEOUT = 3000;

    public ZookeeperClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    // ================= CONNECT =================
    public void connect() throws IOException, InterruptedException {

        zooKeeper = new ZooKeeper(
                host + ":" + port,
                SESSION_TIMEOUT,
                this
        );

        connectedSignal.await();

        createPath("/brokers");
        createPath("/brokers/ids");
        createPath("/topics");
        createPath("/controller");
    }

    // ================= WATCHER =================
    @Override
    public void process(WatchedEvent event) {

        if (event.getState() == Event.KeeperState.SyncConnected) {
            connectedSignal.countDown();
            System.out.println("Connected to ZooKeeper");
        }

        if (event.getState() == Event.KeeperState.Expired) {
            System.out.println("Session expired, reconnecting...");
            try {
                connect();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // ================= CREATE PATH =================
    private void createPath(String path) {
        try {
            if (zooKeeper.exists(path, false) == null) {
                zooKeeper.create(
                        path,
                        new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT
                );
            }
        } catch (Exception ignored) {}
    }

    // ================= EPHEMERAL =================
    public boolean createEphemeralNode(String path, String data) {
        try {
            if (zooKeeper.exists(path, false) == null) {

                zooKeeper.create(
                        path,
                        data.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL
                );

                return true;
            }
        } catch (Exception ignored) {}

        return false;
    }

    // ================= PERSISTENT =================
    public void createPersistentNode(String path, String data) {
        try {
            Stat stat = zooKeeper.exists(path, false);

            if (stat == null) {
                zooKeeper.create(
                        path,
                        data.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT
                );
            } else {
                zooKeeper.setData(path, data.getBytes(), -1);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ================= GET DATA =================
    public String getData(String path) {
        try {
            return new String(zooKeeper.getData(path, false, null));
        } catch (Exception e) {
            return null;
        }
    }

    // ================= WATCH CHILDREN =================
    public void watchChildren(String path, ChildrenCallback callback) {

        try {
            List<String> children = zooKeeper.getChildren(path, event -> {

                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    watchChildren(path, callback);
                }
            });

            callback.onChildrenChanged(children);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // ================= INTERFACE =================
    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }
}