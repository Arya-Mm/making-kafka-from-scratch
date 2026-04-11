package com.simplekafka.broker;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
        // remove /controller persistent path creation so controller can be elected
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
                connectedSignal = new CountDownLatch(1);
                zooKeeper = new ZooKeeper(host + ":" + port, SESSION_TIMEOUT, this);
                connectedSignal.await();
                System.out.println("Reconnected to ZooKeeper");
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
        } catch (Exception ignored) {
        }
    }

    private void ensurePath(String path) throws KeeperException, InterruptedException {
        if (path == null || path.isEmpty() || "/".equals(path)) {
            return;
        }

        if (zooKeeper.exists(path, false) != null) {
            return;
        }

        int slash = path.lastIndexOf('/');
        if (slash > 0) {
            ensurePath(path.substring(0, slash));
        }

        zooKeeper.create(
                path,
                new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT
        );
    }

    // ================= EPHEMERAL =================
    public boolean createEphemeralNode(String path, String data) {
        try {
            Stat stat = zooKeeper.exists(path, false);
            if (stat == null) {
                zooKeeper.create(
                        path,
                        data.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL
                );
                return true;
            }

            if (stat.getEphemeralOwner() == 0) {
                zooKeeper.delete(path, -1);
                zooKeeper.create(
                        path,
                        data.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.EPHEMERAL
                );
                return true;
            }
        } catch (Exception ignored) {
        }

        return false;
    }

    // ================= PERSISTENT =================
    public void createPersistentNode(String path, String data) {
        try {
            int slash = path.lastIndexOf('/');
            if (slash > 0) {
                ensurePath(path.substring(0, slash));
            }

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
        return getData(path, false);
    }

    public String getData(String path, boolean watch) {
        try {
            byte[] data = zooKeeper.getData(path, watch, null);
            return data == null ? null : new String(data);
        } catch (Exception e) {
            return null;
        }
    }

    public boolean exists(String path) {
        try {
            return zooKeeper.exists(path, false) != null;
        } catch (Exception e) {
            return false;
        }
    }

    public void deleteNode(String path) {
        try {
            if (zooKeeper.exists(path, false) != null) {
                zooKeeper.delete(path, -1);
            }
        } catch (Exception e) {
            e.printStackTrace();
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

    public List<String> getChildren(String path) {
        try {
            return zooKeeper.getChildren(path, false);
        } catch (Exception e) {
            return Collections.emptyList();
        }
    }

    public void watchNode(String path, NodeCallback callback) {
        try {
            zooKeeper.exists(path, event -> {
                if (event.getType() == Event.EventType.NodeDeleted ||
                        event.getType() == Event.EventType.NodeDataChanged ||
                        event.getType() == Event.EventType.NodeCreated) {
                    callback.onNodeChanged();
                    watchNode(path, callback);
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<BrokerInfo> getAllBrokerInfo() {
        List<BrokerInfo> brokers = new ArrayList<>();

        for (String brokerId : getChildren("/brokers/ids")) {
            String raw = getData("/brokers/ids/" + brokerId);
            if (raw == null || raw.isBlank()) {
                continue;
            }
            String[] parts = raw.split(":");
            if (parts.length != 2) {
                continue;
            }

            try {
                brokers.add(new BrokerInfo(
                        Integer.parseInt(brokerId),
                        parts[0],
                        Integer.parseInt(parts[1])
                ));
            } catch (NumberFormatException ignored) {
            }
        }

        return brokers;
    }

    // ================= INTERFACE =================
    public interface ChildrenCallback {
        void onChildrenChanged(List<String> children);
    }

    public interface NodeCallback {
        void onNodeChanged();
    }
}