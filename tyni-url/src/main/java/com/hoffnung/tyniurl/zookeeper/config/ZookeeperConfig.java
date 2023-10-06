package com.hoffnung.tyniurl.zookeeper.config;

import lombok.Getter;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Component
@Getter
public class ZookeeperConfig {

    @Value("${zookeeper.host}")
    private String host;

    @Value("${zookeeper.host}")
    private String path;
    private ZooKeeper zoo;
    final CountDownLatch connectedSignal = new CountDownLatch(1);

    // Method to connect zookeeper ensemble.
    public ZooKeeper connect() {
        try {
            zoo = new ZooKeeper(host,5000, we -> {

                if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            });
            connectedSignal.await();
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return zoo;
    }

    // Method to disconnect from zookeeper server
    public void close() throws InterruptedException {
        zoo.close();
    }
}
