package com.longtech.mqtt.Utils;

/**
 * Created by kaiguo on 2018/8/30.
 */

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Tuple;

import java.util.*;

/**
 * Redis缓存实现
 */
public class RedisSession {

    private static final Logger logger = LoggerFactory.getLogger(RedisSession.class);

//    private static final String GLOBAL_REDIS_IP = PropertyFileReader.getItem("global.redis.ip");
//    private static final int GLOBAL_REDIS_PORT = PropertyFileReader.getIntItem("global.redis.port", 6379);
//    private static final int CROSS_REDIS_TIMEOUT = PropertyFileReader.getIntItem("cross.redis.timeout", 3000);
//    private static final int REDIS_PORT = PropertyFileReader.getIntItem("redis.port", "6379");
//    //活动专用redis
//    private static final String ACTIVITY_REDIS_IP = PropertyFileReader.getItem("activity.redis.ip");
//    private static final int ACTIVITY_REDIS_PORT = PropertyFileReader.getIntItem("activity.redis.port", 6379);
//    public static final String ACTIVITY_REDIS_KEY = "ACTIVITY";
//    private Jedis rs = null;
    private boolean autoClose;
    private RedisConnectionMode mode;
    
    private RedisSessionUtil _redisSessionUtil = null;

    public enum RedisConnectionMode {
        NOT_POOL, POOL, CLUSTER
    }

    public RedisSession(RedisSessionUtil redisSessionUtil) {
        this(redisSessionUtil,true);
    }

    public RedisSession(RedisSessionUtil redisSessionUtil, boolean autoClose) {
        _redisSessionUtil = redisSessionUtil;
//        rs = _redisSessionUtil.getResource();
        this.autoClose = autoClose;
        mode = RedisConnectionMode.POOL;
    }

    public void close(Jedis rs) {
        if(rs == null ) {
            return;
        }
        if(mode == RedisConnectionMode.NOT_POOL) {
            rs.close();
        } else if(mode == RedisConnectionMode.POOL) {
            _redisSessionUtil.close(rs);
        }
    }

    public String set(String key, String field) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            String ret = rs.set(key, field);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public int hsetnx (String key, String field, String value){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            long result = rs.hsetnx(key, field, value);
            return (int)result;
        }finally{
            if(autoClose){
                close(rs);
            }
        }
    }

    public Set<String> sMember(String key){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            Set<String> set = rs.smembers(key);
            return set;
        }finally{
            if(autoClose){
                close(rs);
            }
        }
    }

    public long sRem(String key, String... value){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            return rs.srem(key, value);
        }finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public boolean sIsMember(String key, String member) {
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            return rs.sismember(key, member);
        }finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public long sAdd(String key, String... value){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            return rs.sadd(key, value);
        }finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public void del(byte[] key){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            rs.del(key);
        }finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public void del(String key){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            rs.del(key);
        }finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public long del(List<String> keys) {
        Jedis rs = null;
        try {
            if(keys == null || keys.isEmpty()){
                return 0;
            }
            String[] keyArr = keys.toArray(new String[keys.size()]);
            rs = _redisSessionUtil.getResource();
            return rs.del(keyArr);
        } catch (Throwable e) {
            logger.error("REDIS ERROR", e);
//            COKLoggerFactory.monitorException("redis", ExceptionMonitorType.REDIS, COKLoggerFactory.ExceptionOwner.LFK, e);
        } finally {
            if (autoClose) {
                close(rs);
            }
        }
        return 0;
    }
    public long lPush(byte[] key, byte[] value){
        long ret = 0;
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            ret = rs.lpush(key, value);
        }catch(Exception e){
            _redisSessionUtil.closeBroken(rs);
        }finally{
            if(autoClose){
                close(rs);
            }
        }
        return ret;
    }

    public long lPush(String key, String value){
        long ret = 0;
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            ret = rs.lpush(key, value);
        }catch(Exception e){
            _redisSessionUtil.closeBroken(rs);
        }finally{
            if(autoClose){
                close(rs);
            }
        }
        return ret;
    }

    public String rPop(String key){
        String ret = null;
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            ret = rs.rpop(key);
        }catch(Exception e){
            _redisSessionUtil.closeBroken(rs);
        }finally{
            if(autoClose){
                close(rs);
            }
        }
        return ret;
    }

    public String lPop(String key){
        String ret = null;
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            ret = rs.lpop(key);
        }catch(Exception e){
            _redisSessionUtil.closeBroken(rs);
        }finally{
            if(autoClose){
                close(rs);
            }
        }
        return ret;
    }

    public String rPoplPush(String skey, String dkey){
        String ret = null;
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            ret = rs.rpoplpush(skey, dkey);
        }catch(Exception e){
            _redisSessionUtil.closeBroken(rs);
        }finally{
            if(autoClose){
                close(rs);
            }
        }
        return ret;
    }

    public void lRem(byte[] key, int count, byte[] field ){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            rs.lrem(key, count, field);
        }finally{
            if(autoClose)
                close(rs);
        }
    }

    public Set<String> keys(String pattern){
        Set<String> ret;
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            ret = rs.keys(pattern);
        }finally{
            if(autoClose)
                close(rs);
        }
        return ret;
    }

    public String lIndex(String key, long count){
        String ret;
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            ret = rs.lindex(key, count);
        }finally{
            if(autoClose)
                close(rs);
        }
        return ret;
    }

    public long rPush(String key, String value){
        long ret = 0;
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            ret = rs.rpush(key, value);
        }finally{
            if(autoClose){
                close(rs);
            }
        }
        return ret;
    }

    public String get(String key) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            String ret = rs.get(key);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public long incrBy(String key, long incValue){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            long ret =  rs.incrBy(key, incValue);
            return ret;
        }finally{
            if(autoClose){
                close(rs);
            }
        }
    }

    public Double zincrby(String key, double score, String member) {
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            Double ret =  rs.zincrby(key, score, member);
            return ret;
        }finally{
            if(autoClose){
                close(rs);
            }
        }
    }

    public long incr(String key){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            long ret =  rs.incr(key);
            return ret;
        }finally{
            if(autoClose){
                close(rs);
            }
        }
    }

    public long decr(String key){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            long ret =  rs.decr(key);
            return ret;
        }finally{
            if(autoClose){
                close(rs);
            }
        }
    }

    public byte[] hGet(byte[] key, byte[] field){
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            byte[] ret = rs.hget(key, field);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public String hGet(String key, String field) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            String ret = rs.hget(key, field);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public List<String> hMGet(String key, String ... field) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            List<String> ret = rs.hmget(key, field);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public List<String> hMGet(String key, Collection<String> fields) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            String[] arr = new String[fields.size()];
            arr = fields.toArray(arr);
            List<String> ret = rs.hmget(key, arr);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }

        }
    }

    public HashMap<String, String> hMGetMap(String key, List<String> fields) {
        Jedis rs = null;
        try {

            String[] arr = new String[fields.size()];
            int i = 0;
            for(String item : fields) {
                arr[i++] = item;
            }
            //ret数量与fields数量一致，不存在的field会返回空，但数量不会缺
            rs = _redisSessionUtil.getResource();
            List<String> ret = rs.hmget(key, arr);
            HashMap retMap = new HashMap();
            i = 0;
            for(String item : fields) {
                if (StringUtils.isNotBlank(ret.get(i))) {
                    retMap.put(item, ret.get(i));
                }
                i++;
            }
            return retMap;
        } finally {
            if(autoClose) {
                close(rs);
            }

        }
    }

    public Map<String, String> hMGetAll(String key) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            Map<String, String> ret = rs.hgetAll(key);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public void setExpireTime(String key, int seconds) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            rs.expire(key, seconds);
        } finally {
            if(autoClose) {
                close(rs);
            }

        }
    }

    public void setExpireAtTime(String key, int seconds) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            rs.expireAt(key, seconds);
        } finally {
            if(autoClose) {
                close(rs);
            }

        }
    }

    public String hMSet(String key, String nestedKey, String value) {
        Map<String, String> map = new HashMap<>();
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            map.put(nestedKey, value);
            String ret = rs.hmset(key, map);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public String hMSet(String key, Map<String, String> map) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            String ret = rs.hmset(key, map);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public void mset(Map<String, String> map) {
        if (map == null || map.size() == 0) {
            return;
        }
        String keyValues[] = new String[map.size() * 2];
        int index = 0;
        for (Map.Entry<String, String> mapEntry : map.entrySet()) {
            keyValues[index++] = mapEntry.getKey();
            keyValues[index++] = mapEntry.getValue();
        }
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            rs.mset(keyValues);
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public long hDel(String key, List<String> fields) {
        return hDel(key, fields.toArray(new String[fields.size()]));
    }

    public long hDel(String key, String ... fields) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            long ret = rs.hdel(key, fields);
            return ret;
        }
        finally {
            if(autoClose) {
                close(rs);
            }

        }
    }

    public long hSet(byte[] key, byte[] field, byte[] value){
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            long ret = rs.hset(key, field, value);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }


    public long hSet(String key, String field, String value) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            long ret = rs.hset(key, field, value);
            return ret;
        }finally {
            if(autoClose) {
                close(rs);
            }
        }
    }
    public Set<byte[]> hKeys(byte[] key){
        Set<byte[]> fields = new HashSet<>();
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            fields = rs.hkeys(key);
        }finally{
            if(autoClose){
                close(rs);
            }
        }
        return fields;
    }

    public Set<String> hKeys(String key){
        Set<String> fields = new HashSet<>();
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            fields = rs.hkeys(key);
        }finally{
            if(autoClose){
                close(rs);
            }
        }
        return fields;
    }

    public long hLen(String key){
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            long length = rs.hlen(key);
            return length;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public boolean hExists(String key, String field){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            boolean ret = rs.hexists(key, field);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public boolean exists(String key){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            boolean ret = rs.exists(key);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public long pushListFromLeft(String key, String... members) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            long ret = rs.lpush(key, members);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public String lSet(String key, long index, String members) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            String ret = rs.lset(key, index, members);
            return ret;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public List<String> getRangeList(String key, int start, int end){
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            List<String> retList = rs.lrange(key, start, end);
            return retList;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public long getLength(String key){
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            long length = rs.llen(key);
            return length;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public List<byte[]> getRangeList(byte[] key, int start, int end) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            List<byte[]> retList = rs.lrange(key, start, end);
            return retList;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public void lRem(String key, int count, String value){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            rs.lrem(key, count, value);
        }finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public void lTrim(String key, int start, int end){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            rs.ltrim(key, start, end);
        }finally{
            if(autoClose){
                close(rs);
            }
        }
    }

    public String deleteListFromRight(String key) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            String retList = rs.rpop(key);
            return retList;
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public long hincrBy(String key, String field, long num) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return rs.hincrBy(key, field, num);
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public long zAdd(String key, String member, long score){
        Jedis rs = null;
        try{
            rs = _redisSessionUtil.getResource();
            return rs.zadd(key, (double) score, member);
        } finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public long zAdd(String key, Map<String, Double> scoreMap){
        Jedis rs = null;
        try {

            if(scoreMap == null || scoreMap.isEmpty()){
                return 0;
            }
            rs = _redisSessionUtil.getResource();
            return rs.zadd(key, scoreMap);
        } catch (Throwable e) {
            logger.error("REDIS ERROR zAdd:" + key, e);
            //COKLoggerFactory.monitorException("redis", ExceptionMonitorType.REDIS, COKLoggerFactory.ExceptionOwner.LFK, e);
            return 0;
        } finally {
            if (autoClose) {
                close(rs);
            }
        }
    }

    public Double zScore(String key, String member){
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return rs.zscore(key, member);
        } finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public Long zCount(String key, long startTime, long endTime){
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return rs.zcount(key, startTime, endTime);
        } finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public Long zGetRankByAsc(String key, String member){
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return rs.zrank(key, member);
        } finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public Long zGetRankByDesc(String key, String member){
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return (rs.zrevrank(key, member));
        } finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public long zRemByRank(String key, long start, long end){
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return rs.zremrangeByRank(key, start, end);
        } catch (Throwable e) {
            logger.error("REDIS ERROR zRemByRank:" + key + " " + start + " " + end, e);
//            COKLoggerFactory.monitorException("redis", ExceptionMonitorType.REDIS, COKLoggerFactory.ExceptionOwner.LFK, e);
            return 0;
        } finally {
            if (autoClose) {
                close(rs);
            }
        }
    }


    public long zRem(String key, String member){
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return rs.zrem(key, member);
        } catch (Throwable e) {
            logger.error("REDIS ERROR zREM:" + key + " " + member);
//            COKLoggerFactory.monitorException("redis", ExceptionMonitorType.REDIS, COKLoggerFactory.ExceptionOwner.LFK, e);
            return 0;
        } finally {
            if (autoClose) {
                close(rs);
            }
        }
    }

    public Set<String> zRangeByRankAsc(String key, long start, long end){
        Set<String> ret = new LinkedHashSet<>();
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            ret = rs.zrange(key, start, end);
            return ret;
        } catch (Throwable e) {
            logger.error("REDIS ERROR zRangeByRankAsc:" + key + " " + start + " " + end);
//            COKLoggerFactory.monitorException("redis", ExceptionMonitorType.REDIS, COKLoggerFactory.ExceptionOwner.LFK, e);
            return ret;
        } finally {
            if (autoClose) {
                close(rs);
            }
        }
    }

    public Set<String> zRange(String key, long start, long end){
        Set<String> ret = new LinkedHashSet<>();
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            ret = rs.zrange(key, start, end);
            return ret;
        } catch (Throwable e) {
            logger.error("REDIS ERROR zRangeByRankAsc:" + key + " " + start + " " + end);
//            COKLoggerFactory.monitorException("redis", ExceptionMonitorType.REDIS, COKLoggerFactory.ExceptionOwner.LFK, e);
            return ret;
        } finally {
            if (autoClose) {
                close(rs);
            }
        }
    }

    public HashMap<String, Double> zRangeByAsc(String key, long start, long end){
        Jedis rs = null;
        try {
            HashMap<String, Double> ret = new LinkedHashMap<>();
            rs = _redisSessionUtil.getResource();
            Set<Tuple> result = rs.zrangeWithScores(key, start, end);
            if(result != null && result.size() > 0){
                for(Tuple member : result){
                    ret.put(member.getElement(), member.getScore());
                }
            }
            return ret;
        } finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public HashMap<String, Double> zRangeByDesc(String key, long start, long end){
        Jedis rs = null;
        try {
            HashMap<String, Double> ret = new LinkedHashMap<>();
            rs = _redisSessionUtil.getResource();
            Set<Tuple> result = rs.zrevrangeWithScores(key, start, end);
            if(result != null && result.size() > 0){
                for(Tuple member : result){
                    ret.put(member.getElement(), member.getScore());
                }
            }
            return ret;
        } finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public LinkedHashMap<String, Long> zRangeWithScores(String key, long start, long end){
        LinkedHashMap<String, Long> ret = new LinkedHashMap<>();
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            Set<Tuple> result =  rs.zrangeWithScores(key, start, end);

            for(Tuple member : result){
                ret.put(member.getElement(), (long)member.getScore());
            }
        } finally {
            if(autoClose){
                close(rs);
            }
        }
        return ret;
    }



    public Set<String> zRevRangeByScore(String key, long start, long end, int offset, int count){
        Set<String> result = null;
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            result =  rs.zrevrangeByScore(key, start, end, offset, count);

        } finally {
            if(autoClose){
                close(rs);
            }
        }
        return result;
    }

    public Set<String> zRevRange(String key, long start, long end){
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            Set<String> result = rs.zrevrange(key, start, end);
            return result;
        } finally {
            if(autoClose){
                close(rs);
            }
        }
    }

    public void subscribe(JedisPubSub listener, List<String> channels) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            rs.subscribe(listener, channels.toArray(new String[]{}));
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }

    public long publish(String channel, String msg) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return rs.publish(channel, msg);
        } finally {
            if(autoClose) {
                close(rs);
            }
        }
    }



//    public boolean isConnected() {
//        Jedis rs = null;
//        return rs.isConnected();
//    }

    public void setPExpireTime(String key, long time) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            rs.pexpireAt(key, time);
        } catch (Throwable e) {
            logger.error("REDIS ERROR setPExpireTime:" + key + " " + time);
            //COKLoggerFactory.monitorException("redis", ExceptionMonitorType.REDIS, COKLoggerFactory.ExceptionOwner.LFK,
            //        e);
        } finally {
            if (autoClose) {
                close(rs);
            }

        }
    }

    public String type(String key) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return rs.type(key);
        } finally {
            if (autoClose) {
                close(rs);
            }
        }
    }

    public long pttl(String key) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return rs.pttl(key);
        } finally {
            if (autoClose) {
                close(rs);
            }
        }
    }

    public long ttl(String key) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return rs.ttl(key);
        } finally {
            if (autoClose) {
                close(rs);
            }
        }
    }

    public long scard(String key) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return rs.scard(key);
        } finally {
            if (autoClose) {
                close(rs);
            }
        }
    }

    public long zcard(String key) {
        Jedis rs = null;
        try {
            rs = _redisSessionUtil.getResource();
            return rs.zcard(key);
        } finally {
            if (autoClose) {
                close(rs);
            }
        }
    }

    public Jedis getJedis() {
        return _redisSessionUtil.getResource();
    }

    public void releaseJedis(Jedis jedis) {
        if( autoClose ) {
            close(jedis);
        }

    }

}