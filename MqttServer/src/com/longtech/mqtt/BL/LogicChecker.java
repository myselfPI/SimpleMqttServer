package com.longtech.mqtt.BL;

import com.longtech.mqtt.MqttSession;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kaiguo on 2019/5/30.
 */
public class LogicChecker {
    private ConcurrentHashMap<String, MqttSession> clientids = new ConcurrentHashMap<>();

    private static LogicChecker _instance = null;

    public static LogicChecker getInstance() {
        if( _instance == null ) {
            _instance = new LogicChecker();
        }
        return _instance;
    }

    public static void init() {
        LogicChecker.getInstance();
    }

    public void addClientid(String clientid, MqttSession session) {
        MqttSession oldSession = null;
        synchronized (clientids) {
            oldSession = clientids.get(clientid);
            if( oldSession == session ) {
                oldSession = null;
            }
            else if( oldSession != null ) {
                if( oldSession.getTimestamp() <= session.getTimestamp() ) {
                    clientids.put(clientid, session);
                }
                else {
                    oldSession = session;
                }
            }
            else {
                clientids.put(clientid, session);
            }
        }

        if( oldSession != null ) {
            oldSession.setAbnormalExit(false);
            oldSession.kick();

        }
    }

    public void removeClientid( String clientid, MqttSession session ) {
        synchronized (clientids) {
            MqttSession oldSession = clientids.get(clientid);
            if( oldSession == session ) {
                clientids.remove(clientid);
            }
        }
    }
}
