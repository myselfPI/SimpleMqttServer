package com.longtech.mqtt.cluster;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by kaiguo on 2018/12/20.
 */
public class TopicManager {
    private static TopicManager _instance = null;
    private final int THREAD_COUNT = 16;
    private ExecutorService[] workingServices = initThreadGroup(THREAD_COUNT);


    public ExecutorService[] initThreadGroup(int count) {
        ExecutorService[] workingServices = new ExecutorService[count];
        for( int i = 0; i < count; i++) {
            workingServices[i] =  Executors.newSingleThreadExecutor();
        }
        return  workingServices;
    }

    public static void init() {
        if( _instance == null ) {
            TopicManager manger = new TopicManager();
            _instance = manger;
        }
    }


    private ConcurrentHashMap<String, String> topicMap = new ConcurrentHashMap<>();

    public static TopicManager getInstance() {
        return _instance;
    }


    public void addSubTopic(final String topic, long sid) {

//        workingServices[(int)(sid%workingServices.length)].execute(new Runnable() {
//            @Override
//            public void run() {
//                topicMap.put(topic, "1");
//                NodeManager.getInstance().sendAddTopic(topic);
//            }
//        });

    }

    public void removeSubTopic(final String topic, long sid) {




//        workingServices[(int)(sid%workingServices.length)].execute(new Runnable() {
//            @Override
//            public void run() {
//                NodeManager.getInstance().sendRemoveTopic(topic);
//                topicMap.remove(topic);
//
//            }
//        });

    }

    public Collection<String> getAllTopic() {
        ArrayList<String> ret = new ArrayList<>();
        for(Map.Entry<String, String> item : topicMap.entrySet() ) {
            ret.add(item.getKey());
        }
        return ret;
    }

}
