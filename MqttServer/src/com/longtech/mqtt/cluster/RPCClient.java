package com.longtech.mqtt.cluster;

import com.longtech.mqtt.MqttClientWorker;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by kaiguo on 2018/12/20.
 */
public class RPCClient {

    public interface StateListener {
        public void online();
        public void offline();
    }

    protected static Logger logger = LoggerFactory.getLogger(RPCClient.class);
    private ExecutorService workingService = Executors.newSingleThreadExecutor();
    private String mServerAddress = "";

    private RPCClient.StateListener mlistener = null;

    public void setStateListener( StateListener listener ) {
        mlistener = listener;
    }



    public void start( String server ) {
        mServerAddress = server;
        workingService.execute(new Runnable() {
            @Override
            public void run() {
                initMqttClient(false);
            }
        });
    }

    public void stop() {
        try {
            mqttClient.disconnectForcibly();
            mqttClient.close();
        } catch (Exception ex ) {

        }
        synchronized (objLock) {
            mqttClient = null;
        }
    }

    public void SendMessage(final String topic, final String message) {
        workingService.execute(new Runnable() {
            @Override
            public void run() {
                if( mqttClient.isConnected()) {
                    try {
                        mqttClient.publish(topic, message.getBytes(Charset.forName("UTF8")), 0, false);
                    } catch (MqttException e) {
                        e.printStackTrace();
                    }
                }
            }
        });
    }

    MqttClient mqttClient = null;
    private Object objLock = new Object();
    public void initMqttClient( final boolean isReconnect) {
        synchronized (objLock) {
            try {
                mqttClient = new MqttClient(
                        mServerAddress, //URI
                        MqttClient.generateClientId() + "_cluster", //ClientId
                        new MemoryPersistence()); //Persistence
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        MqttConnectOptions options = new MqttConnectOptions();
        options.setUserName("cluster");
        String pwd = "eyJ0eXBlIjoiSldUIiwiYWxnIjoiSFMyNTYifQ.eyJuYW1lIjoiYm9iIiwgImFnZSI6NTAsInNhbHQiOjEyMzA1NDcxNTl9.KZ0334RHpdL3P00ORsbkS-LK-hNfrRCal_xGc_3nd-8";
        char[] pwdBuf = new char[pwd.length()];
        pwd.getChars(0, pwd.length(), pwdBuf, 0);
        options.setPassword(pwdBuf);
        options.setCleanSession(true);
        options.setKeepAliveInterval(90);

        mqttClient.setCallback(new MqttCallback() {
            public void connectionLost(Throwable cause) {

                synchronized (objLock) {
                    mqttClient = null;
                }
                workingService.execute(new Runnable() {
                    @Override
                    public void run() {
                        if(mlistener != null ) {
                            mlistener.offline();
                        }
                    }
                });
                workingService.execute(new Runnable() {
                    @Override
                    public void run() {
                        initMqttClient(false);
                    }
                });
            }

            public void messageArrived(final String topic, final MqttMessage message) throws Exception {

                MqttClientWorker.getInstance().deleveryMessage(topic, message.getPayload());

//                decodeService.execute(new Runnable() {
//                    @Override
//                    public void run() {
//                        processMessage(topic,message.getPayload());
//                    }
//                });
            }

            public void deliveryComplete(IMqttDeliveryToken token) {

            }
        });

        try {
            if( !isReconnect ) {
                logger.info("BeginConnect to Server " + mServerAddress);
            }

            mqttClient.setTimeToWait(1000 * 5);
            mqttClient.connect(options);
            logger.info("Connected" + mServerAddress);

        } catch (Exception ex) {
            if( !isReconnect ) {
                logger.info("Failed Connect " + mServerAddress);
            }

//            ex.printStackTrace();
        }

        try {
            if( mqttClient.isConnected() ) {
                if( mlistener != null ) {
                    mlistener.online();
                }
            } else {
                workingService.execute(new Runnable() {
                    @Override
                    public void run() {
                        initMqttClient(true);
                    }
                });
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            logger.info("subscribe failed");
        }
    }

}
