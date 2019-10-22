package com.longtech.mqtt;

import com.alibaba.fastjson.annotation.JSONField;
import com.longtech.mqtt.Utils.DiskUtilMap;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.MqttPublishMessage;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kaiguo on 2018/12/13.
 */
public class MqttSession implements java.lang.Comparable<MqttSession> {

    protected static Logger logger = LoggerFactory.getLogger(MqttSession.class);
    protected static AtomicLong SESSION_ID_GENRATOR = new AtomicLong();
    protected long sid = 0;
    protected int msgid = 0;

    protected int sessionType = 1;

    public MqttSession() {
        sid =(SESSION_ID_GENRATOR.incrementAndGet());
    }


    protected AtomicInteger messageid = new AtomicInteger(1);

    protected long session_timestamp = 0;

    protected HashSet<String> topics = new HashSet<>();
    protected HashSet<String> wildChardTopics = new HashSet<>();

    public int getSessionType() {
        return sessionType;
    }

    public void generateTimestamp() {
        long timestamp = System.currentTimeMillis();
        ThreadLocalRandom generator = ThreadLocalRandom.current();
        long randnum = generator.nextLong(1000);
        session_timestamp = timestamp *1000 + randnum;
    }

    public long getTimestamp() {
        return session_timestamp;
    }

    public void sendData( final String topic, final byte[] data ) {
        context.executor().execute(new Runnable() {
            @Override
            public void run() {
                if( context.channel().isActive()) {
                    MqttPublishMessage pubMsg = MqttServerHandler.buildPublish(topic, data, messageid.getAndIncrement());
                    context.writeAndFlush(pubMsg);
                    logger.debug("Session {} Send Client: {},{}", getClientid(), topic, data.length);
                    SystemMonitor.send_count.incrementAndGet();
                }
            }
        });

    }

    public void publicOnlineEvent() {
        String topic = "$SYS/brokers/nettyNode/clients/" + clientid +  "/connected";
        String content = "{\"ipaddress\":\"" + ipaddress + "\"}";
        MqttClientWorker.getInstance().publicMessage(topic, content.getBytes(), this, 1);
//        DiskUtilMap.put(clientid, ipaddress);
    }

    public void publicOfflineEvent() {
        String topic = "$SYS/brokers/nettyNode/clients/" + clientid +  "/disconnected";
        String content = "{}";
        MqttClientWorker.getInstance().publicMessage(topic, content.getBytes(), this, 1);
//        DiskUtilMap.remove(clientid);
    }


    public void publicWillMessage() {
        if( !this.isKick && this.abnormalExit && !StringUtil.isNullOrEmpty(this.willTopic)) {
            if( this.willMessage != null && this.willMessage.length > 0) {
                MqttClientWorker.getInstance().publicMessage(this.willTopic, this.willMessage, this, 1);
            }
        }
    }

    public void subTopics(String topic) {
        if( MqttWildcardTopicManager.isWildcardTopic(topic)) {
            synchronized (wildChardTopics) {
                wildChardTopics.add(topic);
            }
            MqttWildcardTopicManager.getInstance().addTopic(topic,this);
        }
        else {
            synchronized (topics) {
                topics.add(topic);
            }
        }

        MqttClientWorker.getInstance().subscribe(topic, this);
    }

    public void unSubTopics(String topic) {
        if( wildChardTopics.contains(topic)) {
            MqttWildcardTopicManager.getInstance().removeTopic(topic,this);
        }
        else {
            synchronized (topics) {
                topics.remove(topic);
            }

        }

        MqttClientWorker.getInstance().unSubscribe(topic,this);
    }

    public void unSubAllTopics() {
        HashSet<String> tmpTopics = new HashSet<>();
        synchronized (topics) {
            tmpTopics.addAll(topics);
        }
        for(String topic: tmpTopics) {
            MqttClientWorker.getInstance().unSubscribe(topic,this);
        }
        tmpTopics.clear();
        synchronized (wildChardTopics) {
            tmpTopics.addAll(wildChardTopics);
        }
        for(String topic: tmpTopics) {
            MqttClientWorker.getInstance().unSubscribe(topic,this);
            MqttWildcardTopicManager.getInstance().removeTopic(topic, this);
        }
    }


    private String clientid;
    private int port;

    protected ChannelHandlerContext context;

    private String ipaddress = "127.0.0.1";

    public String getClientid() {
        return clientid;
    }

    public void setClientid(String clientid) {
        this.clientid = clientid;
    }

//    public ChannelHandlerContext getContext() {
//        return context;
//    }

    public void setContext(ChannelHandlerContext context) {
        this.context = context;
    }

    protected boolean isKick = false;
    public void kick() {
        if( context.channel().isActive()) {
            context.channel().writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            logger.debug("Session {} kick out");
        }
        isKick = true;
    }

    public String getIpaddress() {
        return ipaddress;
    }

    public void setIpaddress(String ipaddress) {
        this.ipaddress = ipaddress;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    public int compareTo(MqttSession o) {
        return Long.compare(this.getSid(), o.getSid());
    }

    public long getSid() {
        return sid;
    }

    public void setSid(long sid) {
        this.sid = sid;
    }

    public String toString() {
        StringBuffer sb = new StringBuffer();
        if ( sessionType == 2) {
            sb.append("clustNode,");
        }
        sb.append(sid);
        sb.append(",");
        sb.append(clientid);
        sb.append(",");
        sb.append(ipaddress).append(":").append(port);
        sb.append(",");
        for( String item: topics) {
            sb.append(item);
            sb.append(",");
        }
        for( String item: wildChardTopics) {
            sb.append(item);
            sb.append(",");
        }
        return sb.toString();
    }

    private String willTopic = null;
    private byte[] willMessage = null;

    private boolean abnormalExit = true;


    public String getWillTopic() {
        return willTopic;
    }

    public void setWillTopic(String willTopic) {
        this.willTopic = willTopic;
    }

    public byte[] getWillMessage() {
        return willMessage;
    }

    public void setWillMessage(byte[] willMessage) {
        this.willMessage = willMessage;
    }

    public boolean isAbnormalExit() {
        return abnormalExit;
    }

    public void setAbnormalExit(boolean abnormalExit) {
        this.abnormalExit = abnormalExit;
    }
}
