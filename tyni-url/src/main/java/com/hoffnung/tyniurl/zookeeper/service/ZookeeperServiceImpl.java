package com.hoffnung.tyniurl.zookeeper.service;

import com.hoffnung.tyniurl.zookeeper.config.ZookeeperConfig;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.stereotype.Service;

import java.io.File;

@Service
public class ZookeeperServiceImpl implements ZookeeperService{

    private static ZooKeeper zookeeper;


    private final ZookeeperConfig zookeeperConfig;

    public ZookeeperServiceImpl(ZookeeperConfig zookeeperConfig) {
        this.zookeeperConfig = zookeeperConfig;
        this.zookeeper = zookeeperConfig.connect();
    }

    public void create(byte[] data, String nodeName){
        try {
            var path = zookeeperConfig.getPath() + File.separator + nodeName;
            zookeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
