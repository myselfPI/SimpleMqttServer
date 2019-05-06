package com.longtech.mqtt.cluster;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.longtech.mqtt.MqttServerHandler;
import com.longtech.mqtt.MqttSession;
import io.netty.handler.codec.mqtt.MqttPublishMessage;

/**
 * Created by kaiguo on 2018/12/20.
 */
public class MqttServerNodeSession extends MqttSession {

    public MqttServerNodeSession() {
        sid =(SESSION_ID_GENRATOR.incrementAndGet());
        sessionType = 2;

    }

    private String nodeAddress = "";



    @Override
    public void sendData( String topic, byte[] data ) {
        MqttPublishMessage pubMsg = MqttServerHandler.buildPublish(topic, data, messageid.getAndIncrement());
        if( this.context.channel().isActive()) {
            context.writeAndFlush(pubMsg);
            logger.debug("ServerSess {} Send Client: {},{}", getClientid(), topic, data.length);
        }
    }

    public void receivedPublishedData(String topic, byte[] data) {
        if( topic.equals("/cluster/addTopicBatch") ) {
            JSONObject obj = JSONObject.parseObject(new String(data));
            JSONArray array = obj.getJSONArray("obj");
            if( array != null ) {
                for( Object item : array) {
                    String topicItem = (String)item;
                    if( topicItem != null ) {
                        this.subTopics(topicItem);
                    }
                }
            }
        }
        else if ( topic.equals("/cluster/addTopicOne")) {
            this.subTopics(new String(data));
        }
        else if ( topic.equals("/cluster/removeTopicBatch")) {
            JSONObject obj = JSONObject.parseObject(new String(data));
            JSONArray array = obj.getJSONArray("obj");
            if( array != null ) {
                for( Object item : array) {
                    String topicItem = (String)item;
                    if( topicItem != null ) {
                        this.unSubTopics(topicItem);
                    }
                }
            }
        }
        else if ( topic.equals("/cluster/removeTopicOne") ) {
            this.unSubTopics(new String(data));
        }
    }

    public String getNodeAddress() {
        return nodeAddress;
    }

    public void setNodeAddress(String nodeAddress) {
        this.nodeAddress = nodeAddress;
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        if ( sessionType == 2) {
            sb.append("clustNode,");
            sb.append(nodeAddress);
            sb.append(",");
        }
        sb.append(sid);
        sb.append(",");
        sb.append(getClientid());
        sb.append(",");
        sb.append(getIpaddress()).append(":").append(getPort());
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
}
