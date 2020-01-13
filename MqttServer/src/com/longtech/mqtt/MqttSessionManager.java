package com.longtech.mqtt;


import com.longtech.mqtt.Utils.DiskUtilMap;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kaiguo on 2018/12/13.
 */
public class MqttSessionManager {

    private static MqttSessionManager _instance = null;
    public static Logger logger = LoggerFactory.getLogger(MqttSessionManager.class);
    public static void init() {
        _instance = new MqttSessionManager();
    }

    public static MqttSessionManager getInstance() {
        return _instance;
    }

    private ConcurrentHashMap<Channel, MqttSession> sessionMap = new ConcurrentHashMap<>();

    private ConcurrentHashMap<String, MqttSession> clientidMap = new ConcurrentHashMap<>();

    public void addSession( MqttSession session, Channel channel) {

        if( session.getClientid() != null ) {
            clientidMap.put(session.getClientid(), session);
        }

        sessionMap.put(channel, session);
    }

    public MqttSession addSession( MqttSession session, Channel channel,boolean checkClientid) {

        MqttSession oldSession = null;
        if( checkClientid ) {
            logger.debug("GK clients size: {}", clientidMap.size());
            oldSession = clientidMap.put(session.getClientid(), session);
        }
        sessionMap.put(channel, session);
        return oldSession;
    }

    public MqttSession removeSession(Channel channel) {

        MqttSession session = sessionMap.remove(channel);
        if( session != null && session.getClientid() != null ) {
            MqttSession currentSession = clientidMap.get(session.getClientid());
            if( currentSession.context.channel() == channel) {
                clientidMap.remove(session.getClientid());
            }
        }
        return session;
    }

    public MqttSession getDuplicateClientIdSession( String clientid ) {
        return clientidMap.get(clientid);
    }

    public MqttSession getSession(Channel channel) {
        return sessionMap.get(channel);
    }

    public long getCurrentSessions() {
        return sessionMap.size();
    }

    public static void recoveryLastRunData(MqttClientWorker worker) {
        // do nothing
//        ConcurrentHashMap<String, String> testRet = null;
//
//
//        testRet = DiskUtilMap.getDiskMap();
//        if( testRet != null) {
//            for( Map.Entry<String,String> item: testRet.entrySet()) {
//                String topic = "$SYS/brokers/nettyNode/clients/" + item.getKey() +  "/disconnected";
//                String content = "nothing";
//                worker.publicMessage(topic,content.getBytes(), null, 1);
//            }
//        }
    }

    public ConcurrentHashMap<Channel, MqttSession> getSessionMap() {
        return sessionMap;
    }
}
