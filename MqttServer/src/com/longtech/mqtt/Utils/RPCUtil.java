package com.longtech.mqtt.Utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.longtech.mqtt.MqttClientWorker;


import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by kaiguo on 2018/10/16.
 */
public class RPCUtil {

    public interface RPCUtilResult {
        public void apiResult(int code, Object result);
    }

    private static RPCUtil _instance = null;
    public static void init(MqttClientWorker worker) {
        _instance.worker = worker;
    }

    MqttClientWorker worker = null;

    AtomicLong methodId = new AtomicLong();
    class RPCSessionData {
        public RPCUtilResult result;
        public long callTime;
        Object params;
    }
    ConcurrentHashMap<Long, RPCSessionData> rpcStores = new ConcurrentHashMap<>();

    public void callRemoteMethod( String server, Object params, long timeout, TimeUnit unit, RPCUtilResult rpcResult ) {
        long currentMethodId = methodId.incrementAndGet();
        String topic = "r/api/" + server + "/" + currentMethodId + "/" + Constants.HOST_NAME;
        byte[] data = null;
        if( params instanceof Collection || params instanceof Map ) {
            String strParams = JSON.toJSONString(params);
            try {
                data = strParams.getBytes("UTF-8");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        RPCSessionData rpc = new RPCSessionData();
        rpc.result = rpcResult;
        rpc.params = params;
        rpc.callTime = System.currentTimeMillis();
        rpcStores.put(currentMethodId, rpc);
        worker.publicMessage(topic, data, 1);
    }

    public void callRemoteMethodCallback( String topic, byte[] data ) {
        int index1 = topic.lastIndexOf("/");
        String strMethodId = topic.substring(index1 + 1);
        Long methodId = Long.parseLong(strMethodId);

    }


    public void RpcMethodProcess( String topic, byte[] data ) {
        int index1 = topic.lastIndexOf("/");
        String strMethodId = topic.substring(index1 + 1);
        int index2 = topic.lastIndexOf("/", index1 - 1);
        String serverId = topic.substring(index2 + 1, index1 - 1);

        String params = new String(data, Charset.forName("UTF-8"));

        JSONObject jsonobj = JSONObject.parseObject(params);
        String classmethod = (String)jsonobj.get("p");
    }

}
