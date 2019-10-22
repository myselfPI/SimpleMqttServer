package com.longtech.mqtt;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kaiguo on 2018/12/14.
 */
public class SystemMonitor {
    public static AtomicLong reqCount = new AtomicLong();
    public static AtomicLong allReqCount = new AtomicLong();
    public static AtomicLong maxConnectNumber = new AtomicLong();
    public static AtomicLong connectNumber = new AtomicLong();
    public static AtomicLong rps = new AtomicLong();
    private static ScheduledExecutorService mWorkingExecutor =  Executors.newSingleThreadScheduledExecutor();
    public static void init() {
        mWorkingExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                rps.set(reqCount.get() / 2);
                reqCount.set(0);

            }
        }, 2, 2, TimeUnit.SECONDS);
    }


    public static AtomicLong recv_count = new AtomicLong();
    public static AtomicLong send_count = new AtomicLong();
    public static AtomicLong connect_count = new AtomicLong();
}
