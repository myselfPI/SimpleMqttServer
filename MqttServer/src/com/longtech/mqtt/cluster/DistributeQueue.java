package com.longtech.mqtt.cluster;

import com.longtech.mqtt.MqttClientWorker;

import java.util.HashMap;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by kaiguo on 2019/1/25.
 */
public class DistributeQueue {

    private LinkedBlockingDeque<HashMap<String, String>> messageRecvingQueue = new java.util.concurrent.LinkedBlockingDeque<>();


    private MqttClientWorker worker = null;


    public void addData(  HashMap<String, String>  message ) {
        worker.publicMessage("$cluster/q/addmessage", new byte[]{}, 1);
    }

    public void pushSubData( HashMap<String, String>  message ) {

    }

    public void pushUnSubData( HashMap<String, String>  messag ) {

    }

    public void pushBatchSubData() {

    }




}
