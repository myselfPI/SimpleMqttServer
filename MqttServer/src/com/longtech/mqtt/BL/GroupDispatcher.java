package com.longtech.mqtt.BL;

import com.longtech.mqtt.MqttSession;

import java.util.ArrayList;

/**
 * Created by kaiguo on 2019/5/30.
 */
public class GroupDispatcher {
    private static GroupDispatcher _instance = null;

    public static GroupDispatcher getInstance() {
        if( _instance == null ) {
            _instance = new GroupDispatcher();
        }
        return _instance;
    }

    public static void init() {
        GroupDispatcher.getInstance();
    }

    public MqttSession getSuitableSession( String topic, String subtopic, MqttSession session ) {
        if( session.getSessionType() == 3) {

        }
        return session;
    }


}
