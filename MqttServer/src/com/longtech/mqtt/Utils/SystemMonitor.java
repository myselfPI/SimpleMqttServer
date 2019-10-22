package com.longtech.mqtt.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kaiguo on 2018/9/18.
 */
public class SystemMonitor {
    private static  long prevTotal, prevFree;
    private static Logger logger = LoggerFactory.getLogger(SystemMonitor.class);
    private static ScheduledExecutorService mWorkingExecutor =  Executors.newSingleThreadScheduledExecutor();
    public static ConcurrentHashMap<String, Long> cmdsMaxTime = new ConcurrentHashMap<>();
    public static class CmdTime {
        private ConcurrentLinkedQueue<Long> spendTime = new ConcurrentLinkedQueue();
        private AtomicLong count = new AtomicLong();
        public void addTime( Long time ) {
            if( count.get() > 1000) {
                spendTime.poll();
            }
            spendTime.add(time);
            count.incrementAndGet();
        }

        public String getHistoryTime() {
            Object[] allItem = spendTime.toArray();
            if( allItem.length != 0 ) {
                long last10 = 0;
                long last100 = 0;
                long last1000 = 0;
                int j,k,l,jc,kc,lc;
                j = k = l = 1;
                jc = kc = lc = 0;
                for ( int i = allItem.length - 1; i>= 0; i--) {

                    if( j <= 10) {
                        last10 += ( ((Long)allItem[i])== null? 0 : (Long)allItem[i]);
                        jc = j;
                    }
                    if( k <= 100 ) {
                        last100 += ( ((Long)allItem[i])== null? 0 : (Long)allItem[i]);
                        kc = k;
                    }
                    if( l <= 1000 ) {
                        last1000 += ( ((Long)allItem[i])== null? 0 : (Long)allItem[i]);
                        lc = l;
                    }
                    j++;
                    k++;
                    l++;
                }
                String result = jc + ":" + (last10 * 1.0f/ jc) + " " + kc + ":" + (last100 *1.0f/ kc) + " " + lc + ":" + ( last1000 * 1.0f/lc);
                return result;
            }
            return "0 0 0";
        }
    }

    public static ConcurrentHashMap<String, CmdTime> cmdsHistoryTime = new ConcurrentHashMap<>();
    public static void init() {
        int time_period = CommonUtils.getIntValue("monitor_time_interval", 60 * 60);
        prevTotal = Runtime.getRuntime().totalMemory();
        prevFree = Runtime.getRuntime().freeMemory();
        mWorkingExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                Runtime rt =  Runtime.getRuntime();
                long total = rt.totalMemory();
                long free = rt.freeMemory();
                long used = total - free;
                long prevUsed = (prevTotal - prevFree);

                logger.info("RunTime Memory:Total {} Used {} ∆Used {} Free {} ∆Free{}", total,used,(used-prevUsed), free, (free-prevFree));
                prevTotal = total;
                prevFree = free;
            }
        },5,time_period, TimeUnit.SECONDS);

        mWorkingExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                callResult = call_count.getAndSet(0)/2;
                pushResult = push_count.getAndSet(0)/2;
                webResult = web_count.getAndSet(0)/2;
                synchronized (lock) {
                    if( maxCallResult < callResult) {
                        maxCallResult = callResult;
                        maxCallResult_time = CommonUtils.getYmdHi();
                    }

                    if( maxWebResult < webResult) {
                        maxWebResult = webResult;
                        maxWebResult_time = CommonUtils.getYmdHi();
                    }

                    if( maxPushResult < pushResult) {
                        maxPushResult = pushResult;
                        maxPushResult_time = CommonUtils.getYmdHi();
                    }
                }


            }
        }, 2, 2, TimeUnit.SECONDS);
    }

    public static AtomicLong call_count = new AtomicLong();
    public static AtomicLong push_count = new AtomicLong();
    public static AtomicLong web_count = new AtomicLong();
    public static AtomicLong maxCmdProcessTime = new AtomicLong();

    public static long callResult = 0L;
    public static long pushResult = 0L;
    public static long webResult = 0L;
    public static long maxCallResult = 0L;
    public static long maxPushResult = 0L;
    public static long maxWebResult = 0L;
//    public static long maxCmdProcessTime = 0L;
    public static String maxCmdProcessTime_time = "";
    public static String maxCallResult_time = "";
    public static String maxWebResult_time = "";
    public static String maxPushResult_time = "";
    public static Object lock = new Object();

}
