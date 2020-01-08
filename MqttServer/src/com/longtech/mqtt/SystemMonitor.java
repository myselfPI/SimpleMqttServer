package com.longtech.mqtt;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
    public static AtomicBoolean is_debug = new AtomicBoolean(false);
    public static AtomicLong mqtt_tcp_count = new AtomicLong();
    public static AtomicLong mqtt_ssl_count = new AtomicLong();
    public static AtomicLong mqtt_ws_count = new AtomicLong();
    public static AtomicLong mqtt_wss_count = new AtomicLong();
    public static AtomicLong mqtt_cluster_topic = new AtomicLong();
    public static Object objDebuger1 = null;
    public static Object objDebuger2 = null;

    public static void setConnectDetailNumber(MqttSession session, long val) {
        SystemMonitor.connectNumber.getAndAdd(val);
        if( session.getConectPort() == MqttServer.TCP_Port ) {
            mqtt_tcp_count.addAndGet(val);
        }
        else if ( session.getConectPort() == MqttServer.SSL_Port ) {
            mqtt_ssl_count.addAndGet(val);
        }
        else if ( session.getConectPort() == MqttServer.WS_Port ) {
            mqtt_ws_count.addAndGet(val);
        }
        else if ( session.getConectPort() == MqttServer.WSS_Port ) {
            mqtt_wss_count.addAndGet(val);
        }
        else if ( session.getConectPort() == MqttServer.CTL_Port ) {
//            mqtt_ssl_count.addAndGet(val);
        }
    }
}
