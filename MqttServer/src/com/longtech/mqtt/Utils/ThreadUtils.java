package com.longtech.mqtt.Utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by kaiguo on 2018/8/29.
 */
public class ThreadUtils {

    private static ScheduledExecutorService mWorkingExecutor =  Executors.newScheduledThreadPool(8);



    public static void schedule(Runnable runnable, int period, TimeUnit unit ) {
        mWorkingExecutor.schedule(runnable, period, unit);
//        mWorkingExecutor.scheduleAtFixedRate(runnable, period, period, TimeUnit.SECONDS);
//        mWorkingExecutor.shutdown();
    }

    public static void scheduleAtFixRate(Runnable runnable, long initalDelay, long interval, TimeUnit unit) {
        mWorkingExecutor.scheduleWithFixedDelay(runnable,initalDelay,initalDelay, unit);
    }

    private static ExecutorService asyncService = Executors.newFixedThreadPool(16);


    public static void runAsyncTask(Runnable runable) {
        asyncService.execute(runable);
    }
}
