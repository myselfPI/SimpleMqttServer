package com.longtech.mqtt.BL;

import com.longtech.mqtt.Utils.CommonUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kaiguo on 2019/12/10.
 */
public class TopicStore {
    public static ConcurrentHashMap<String,AtomicLong> alltopics = new ConcurrentHashMap<>();
    private static ScheduledExecutorService mWorkingExecutor =  Executors.newScheduledThreadPool(1);
    public static interface Topiclistener {
        void topicSubEvent(String topic);
        void topicUnsubEvent(String topic);
        void dumpSubTopics(String[] topics);
        void pubTopics(String topic, byte[] data);
    }

    public static Topiclistener mListener = null;
    public static void setlistener(Topiclistener topiclistener) {
        mListener = topiclistener;
    };

    public static void subTopic(final String topic) {

        mWorkingExecutor.execute(new Runnable() {
            @Override
            public void run() {
                AtomicLong counts = CommonUtils.getBean(alltopics, topic, AtomicLong.class);
                counts.incrementAndGet();

                if (mListener != null && counts.get() == 1) {
                    mListener.topicSubEvent(topic);
                }
            }
        });
    }

    public static void unsubTopic(final String topic) {

        mWorkingExecutor.execute(new Runnable() {
            @Override
            public void run() {
                AtomicLong counts = CommonUtils.getBean(alltopics,topic,AtomicLong.class);
                if( counts.decrementAndGet() == 0 ) {
                    alltopics.remove(topic);
                }

                if( mListener != null && counts.get() == 0 ) {
                    mListener.topicUnsubEvent(topic);
                }
            }
        });

    }

    public static void publishTopicData(final String topic, final byte[] data) {

        mWorkingExecutor.execute(new Runnable() {
            @Override
            public void run() {

                if( mListener != null  ) {
                    mListener.pubTopics(topic,data);
                }
            }
        });

    }

    public static Map<String,AtomicLong> dumpTopic() {

        TreeMap<String, AtomicLong> topics = new TreeMap<>();
        for(Map.Entry<String, AtomicLong> item : alltopics.entrySet()) {
            topics.put(item.getKey(),item.getValue());
        }
        return topics;
    }

    public static void queryDumpAndMonitorTopics(final Topiclistener listener) {

        mWorkingExecutor.execute(new Runnable() {
            @Override
            public void run() {
                String[] topics = new String[alltopics.size()];
                int i = 0;
                for(Map.Entry<String, AtomicLong> item : alltopics.entrySet()) {
                    topics[i] = item.getKey();
                    i++;
                }

                if( listener != null ) {
                    listener.dumpSubTopics(topics);
                }
                mListener = listener;
            }
        });
    }


}
