package com.longtech.mqtt.Utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * Created by kaiguo on 2018/8/29.
 */
public class RedisSessionUtil {
    private JedisPool jedisPool = null;

    public RedisSessionUtil() {
    }

    public Jedis getResource() {
        return jedisPool.getResource();
    }

    public void close(Jedis resource) {
        jedisPool.returnResource(resource);
    }

    public void closeBroken(Jedis resource){
        jedisPool.returnBrokenResource(resource);
    }

    public void initPool(String ip, int port, int timeout, String name) {
//        String name = "COK" + Constants.SERVER_ID;
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setJmxNamePrefix("pool");
        poolConfig.setMaxTotal(400);
        poolConfig.setMaxIdle(50);
        poolConfig.setMinIdle(10);
        poolConfig.setMaxWaitMillis(3 * 1000);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMinEvictableIdleTimeMillis( 2 * 500);
        poolConfig.setSoftMinEvictableIdleTimeMillis( 2 * 1000);
        poolConfig.setTimeBetweenEvictionRunsMillis(2 * 1000);
        poolConfig.setNumTestsPerEvictionRun(100);
//        poolConfig.setTestOnBorrow(getBooleanItem("redis.pool.testOnBorrow"));
//        poolConfig.setTestOnReturn(getBooleanItem("redis.pool.testOnReturn"));
        jedisPool = new JedisPool(poolConfig, ip, port, timeout, null, Protocol.DEFAULT_DATABASE, name);
    }

//    private static class LazyHolder {
//        private static final RedisSessionUtil instance = new RedisSessionUtil();
//    }
//
//    public static RedisSessionUtil getInstance() {
//        return LazyHolder.instance;
//    }
}
