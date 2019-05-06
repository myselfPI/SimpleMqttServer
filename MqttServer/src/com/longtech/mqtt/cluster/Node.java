package com.longtech.mqtt.cluster;

import com.longtech.mqtt.Utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Created by kaiguo on 2018/12/20.
 */
public class Node {

    protected static final Logger logger = LoggerFactory.getLogger(Node.class);

    public Node() {
    }

    private RPCClient client = new RPCClient();
    private String address = "";
    public void init( String address) {
        this.address = address;

    }

    public String getAddress() {
        return this.address;
    }

    public void sendAddTopicMessageBatch( String message ) {
        client.SendMessage("/cluster/addTopicBatch", message);
        logger.debug("sendAddTopicMessageBatch To {} {}", address, message);
    }

    public void sendAddTopicMessage( String message ) {
        client.SendMessage("/cluster/addTopicOne", message);
        logger.debug("sendAddTopicMessage To {} {}", address, message);
    }

    public void sendRemoveTopicMessageBatch( String message ) {
        client.SendMessage("/cluster/removeTopicBatch", message);
        logger.debug("sendRemoveTopicMessageBatch To {} {}", address, message);
    }

    public void queryRemoteTopicToLocal() {
        client.SendMessage("/cluster/addNode",NodeManager.getInstance().getMyNodeName());
        logger.debug("queryRemoteTopicToLocal To {} {}", address, NodeManager.getInstance().getMyNodeName());
    }

    public void queryRemoteTopicToOtherNode() {
        Collection<String> node = NodeManager.getInstance().getAllNodes();
        StringBuffer sb = new StringBuffer();
        for( String item: node) {
            if( !item.equals(address) && !item.equals(NodeManager.getInstance().getMyNodeName())) {
                client.SendMessage("/cluster/addOtherNode",item);
                sb.append(item).append(",");
            }
        }
        logger.debug("queryRemoteTopicToOtherNode To {} {}", address, sb.toString());
    }

    public void sendRemoveTopicMessage( String message ) {
        client.SendMessage("/cluster/removeTopicOne", message);
        logger.debug("sendRemoveTopicMessage To {} {}", address, message);
    }

    public void start() {
        client.setStateListener(new RPCClient.StateListener() {
            @Override
            public void online() {
                if( mStateListener != null ) {
                    mStateListener.online();
                }
            }

            @Override
            public void offline() {
                if( mStateListener != null ) {
                    mStateListener.offline();
                }
            }
        });
        client.start(address);
    }

    public void stop() {
        client.stop();
    }

    private StateListener mStateListener = null;
    public interface StateListener {
        public void online();
        public void offline();
    }

    public void setStateListener( StateListener listener ) {
        mStateListener = listener;
    }
}
