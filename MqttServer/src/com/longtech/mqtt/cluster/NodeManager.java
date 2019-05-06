package com.longtech.mqtt.cluster;

import com.alibaba.fastjson.JSONObject;
import com.longtech.mqtt.Utils.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kaiguo on 2018/12/20.
 */
public class NodeManager {

    protected static final Logger logger = LoggerFactory.getLogger(NodeManager.class);

    private static NodeManager _instance = null;

    private HashMap<String, Node> nodeData = new HashMap<>();
    private ConcurrentHashMap<String, Node> onlineNodeData = new ConcurrentHashMap<>();

    public static void init() {
        if( _instance == null ) {
            NodeManager manger = new NodeManager();
            manger.myNodeName = CommonUtils.getValue("my_node_name","");
            _instance = manger;

        }
    }

    private String myNodeName = "";



    public static NodeManager getInstance() {
        return _instance;
    }

    public void sendAddTopic(String topic) {
        for(Map.Entry<String, Node> item : onlineNodeData.entrySet() ) {
            item.getValue().sendAddTopicMessage(topic);
        }
    }

    public void sendRemoveTopic(String topic) {

        for(Map.Entry<String, Node> item : onlineNodeData.entrySet() ) {
            item.getValue().sendRemoveTopicMessage(topic);
        }
    }

//    public void sendAllExistTopic(Collection<String> topics) {
//
//    }
//
//    public void processRecevicedAllExistTopic( Collection<String> topics ) {
//
//    }

    public void addNode( final String address) {

        if( onlineNodeData.containsKey(address)) {
            return;
        }

        if( nodeData.containsKey(address)) {
            return;
        }

        logger.debug("AddNode {}", address);

        final Node node = new Node();
        node.init(address);

        nodeData.put(address, node);
        node.setStateListener(new Node.StateListener() {
            @Override
            public void online() {
                logger.debug("node {} online", node.getAddress());
                Collection<String> topics = TopicManager.getInstance().getAllTopic();
                JSONObject obj = new JSONObject();
                obj.put("obj", topics);
                node.sendAddTopicMessageBatch(obj.toJSONString());
                onlineNodeData.put(address, node);
                node.queryRemoteTopicToLocal();
                node.queryRemoteTopicToOtherNode();
            }

            @Override
            public void offline() {
                onlineNodeData.remove(address);
                logger.debug("node {} offline", node.getAddress());
            }
        });
        node.start();
    }

    public Collection<String> getAllNodes() {
        Collection<String> datas = new ArrayList<>();
        for( Map.Entry<String, Node> item :nodeData.entrySet()) {
            datas.add(item.getKey());
        }
        return datas;
    }


    public String getMyNodeName() {
        return myNodeName;
    }

    public void setMyNodeName(String myNodeName) {
        this.myNodeName = myNodeName;
    }
}
