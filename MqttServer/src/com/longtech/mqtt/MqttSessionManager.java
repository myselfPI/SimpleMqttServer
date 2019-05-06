package com.longtech.mqtt;


import com.longtech.mqtt.Utils.DiskUtilMap;
import io.netty.channel.Channel;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kaiguo on 2018/12/13.
 */
public class MqttSessionManager {

    private static MqttSessionManager _instance = null;

    public static void init() {
        _instance = new MqttSessionManager();
    }

    public static MqttSessionManager getInstance() {
        return _instance;
    }

    private ConcurrentHashMap<Channel, MqttSession> sessionMap = new ConcurrentHashMap<>();

    public void addSession( MqttSession session, Channel channel) {
        sessionMap.put(channel, session);
    }

    public MqttSession removeSession(Channel channel) {
        return sessionMap.remove(channel);
    }

    public MqttSession getSession(Channel channel) {
        return sessionMap.get(channel);
    }

    public long getCurrentSessions() {
        return sessionMap.size();
    }

    public static void recoveryLastRunData(MqttClientWorker worker) {
        ConcurrentHashMap<String, String> testRet = null;


        testRet = DiskUtilMap.getDiskMap();
        if( testRet != null) {
            for( Map.Entry<String,String> item: testRet.entrySet()) {
                String topic = "$SYS/brokers/nettyNode/clients/" + item.getKey() +  "/disconnected";
                String content = "nothing";
                worker.publicMessage(topic,content.getBytes(),1);
            }
        }
    }

    public ConcurrentHashMap<Channel, MqttSession> getSessionMap() {
        return sessionMap;
    }
}
