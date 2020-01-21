package com.longtech.mqtt;

/**
 * Created by kaiguo on 2018/12/13.
 */
//import com.im30.chatserver.Constants;
//import com.im30.chatserver.Handler.PushRequestFactory;
//import com.im30.chatserver.Handler.SystemStatRequestFactory;
//import com.im30.chatserver.Session.ISession;
//import com.im30.chatserver.Session.SessionManager;
//import com.im30.chatserver.Utils.CommonUtils;
//import com.im30.chatserver.Utils.SystemMonitor;
import com.longtech.mqtt.BL.GroupDispatcher;
import com.longtech.mqtt.BL.TopicStore;
import com.longtech.mqtt.Utils.CommonUtils;
import com.longtech.mqtt.Utils.Constants;
import com.longtech.mqtt.cluster.TopicManager;
import io.netty.util.internal.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
//import org.jcp.xml.dsig.internal.dom.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by kaiguo on 2018/8/29.
 */
public class MqttClientWorker {

    private static MqttClientWorker _instance = null;
    public static void init() {
        _instance = new MqttClientWorker();
        String isServer = CommonUtils.getValue("use_as_server","false");
        if("true".equals(isServer)) {
            _instance.isServer = true;
        }
        _instance.startClient();
    }
    public static MqttClientWorker getInstance() {
        return _instance;
    }

    private boolean isServer = false;

    private static Logger logger = LoggerFactory.getLogger(MqttClientWorker.class);
    private MqttAsyncClient mClient = null;
    private boolean exitWorker = false;
    private boolean isUsingSSL = false;
    private long lastSendTime = -1L;
    private ConcurrentHashMap<String, String> client_ip = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Boolean> zonesMap = new ConcurrentHashMap<>();
    private ReentrantLock lockClient = new ReentrantLock();
    private int hasConnectborker = 0;
    private static int SplitThreshold = 4096;
    private static int SplitPartSize = 1024;
    private ExecutorService mRecvingWorkingExecutor = null;
    private ScheduledFuture<?> mRecvingWorkingFuture = null;
    private ExecutorService mSendingWorkingExecutor = null;
    private ScheduledFuture<?> mSendingWorkingFuture = null;
    private ScheduledExecutorService mHeartBeatExecutor =  Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> mHeartBeatFuture = null;
    static class DataHolder {
        String topic = "";
        MqttMessage message;
        public DataHolder(String topic, MqttMessage message) {
            this.topic = topic;
            this.message = message;
        }
        int type = 0; // 0 receive 1 send 2 ping 3 connection lost
        byte[] data = null;
        public  DataHolder(String topic, byte[] sendData) {
            type = 1;
            this.topic = topic;
            this.data = sendData;
        }

        public DataHolder(int type) {
            this.type = type;
            if( type == 2) {
                this.topic = "NONE";
                this.data = PING_DATA;
            }
        }
        public final static byte[] PING_DATA = new String("ping").getBytes(Charset.forName("UTF-8"));
    }

    LinkedBlockingDeque<DataHolder> messageRecvingQueue = new LinkedBlockingDeque<DataHolder>();
    LinkedBlockingDeque<DataHolder> messageSendingQueue = new LinkedBlockingDeque<DataHolder>();

    public MqttClientWorker(){
        mRecvingWorkingExecutor = Executors.newSingleThreadExecutor();
        mSendingWorkingExecutor = Executors.newSingleThreadExecutor();
    }

    public int connectStatus() {
        return hasConnectborker;
    }


    public void startClient() {

        if( isServer ) {
            return;
        }
        SystemMonitor.mqtt_cluster_topic.set(0L);
        exitWorker = false;

        Future<?> submit = mRecvingWorkingExecutor.submit(new Runnable() {
            @Override
            public void run() {

            }
        });
        try {
            submit.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        submit = mSendingWorkingExecutor.submit(new Runnable() {
            @Override
            public void run() {

            }
        });
        try {
            submit.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        messageRecvingQueue.clear();
        messageSendingQueue.clear();

        tryConnectAndSubScrible();
        ProcessMessageQueue();
        //专用保活队列
        mHeartBeatFuture = mHeartBeatExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                try {
                    if (messageSendingQueue.size() == 0) {
                        messageSendingQueue.put(new DataHolder(2));
                    }
                } catch (InterruptedException e) {
                }
            }
        }, 30, 30, TimeUnit.SECONDS);


    }

    public final static int single_server = 1;
    public final static int cluster_gate = 2;
    public final static int cluster_center = 3;
    public void setServer( int serverMode ) {
        switch (serverMode) {
            case single_server:
                isServer = true;
                break;
            case cluster_center:
                break;
            case cluster_gate:
                isServer = false;
                break;
        }
    }
    private  String server_address = "";
    public void setServerAddress(String val) {
        server_address = val;
    }
    public String getServerAddress() {
        return server_address;
    }

    private Object lockObj = new Object();
    private ConcurrentHashMap<String,ConcurrentSkipListSet<MqttSession>> subscribledTopics = new ConcurrentHashMap<>();

    public long getSubscribledTopicsSize() {
        return subscribledTopics.size();
    }

    public ConcurrentHashMap<String,ConcurrentSkipListSet<MqttSession>> getSubscribledTopics() {
        return subscribledTopics;
    }

    public void subscribe(String topic, MqttSession session) {

        if( !isServer ) {
//            if( mClient.isConnected()) {
//                try {
//                    mClient.subscribe(topic, 0);
//                } catch (MqttException e) {
//                    e.printStackTrace();
//                }
//            }
        }
        ConcurrentSkipListSet<MqttSession> sessions = CommonUtils.getBean(subscribledTopics, topic, ConcurrentSkipListSet.class);
        sessions.add(session);
        if( session.getSessionType() == 1 ) {
            TopicManager.getInstance().addSubTopic(topic, session.getSid());
        }

        TopicStore.subTopic(topic);
    }

    public void unSubscribe(String topic, MqttSession session) {


        ConcurrentSkipListSet<MqttSession> sessions = CommonUtils.getBean(subscribledTopics, topic, ConcurrentSkipListSet.class);
        sessions.remove(session);
        if( sessions.size() == 0 ) {
            subscribledTopics.remove(topic);
        }
        if( session.getSessionType() == 1 ) {
            TopicManager.getInstance().removeSubTopic(topic, session.getSid());
        }

        if( !isServer ) {
//            if( mClient.isConnected()) {
//                try {
//                    if( sessions.size() == 0 ) {
//                        mClient.unsubscribe(topic);
//                    }
//                } catch (MqttException e) {
//                    e.printStackTrace();
//                }
//            }
        }

        TopicStore.unsubTopic(topic);
    }

    public Collection<MqttSession> getSessions(String topic) {
        HashSet<MqttSession> ret = new HashSet<>();
        ConcurrentSkipListSet<MqttSession> sessions = subscribledTopics.get(topic);
        if( sessions != null ) {
            ret.addAll(sessions);
        }
        return ret;
    }

    public void tryConnectAndSubScrible() {

        if( exitWorker ) {
            return;
        }

        hasConnectborker = 1;
        boolean isConnectSuccess = false;
        boolean isSub1 = false;
        boolean isSub2 = false;
        try {
            if( mClient == null ) {

                String mqttBrokerAddres = server_address;
                if(StringUtil.isNullOrEmpty(mqttBrokerAddres)) {
                    mqttBrokerAddres = CommonUtils.getValue("mqtt_server", "");
                }
                String mqttBrokerAddres_v0 = "";
                SplitThreshold = 4096;
                SplitPartSize = 2048;
                if( mqttBrokerAddres == null || mqttBrokerAddres.isEmpty()) {
                    mqttBrokerAddres = mqttBrokerAddres_v0;
                }

                if(mqttBrokerAddres == null || mqttBrokerAddres.isEmpty() ) {
                    return;
                }

                StringTokenizer st = new StringTokenizer(mqttBrokerAddres,"|");
                ArrayList<String> condidateip = new ArrayList<>();
                while (st.hasMoreTokens()) {
                    String val = st.nextToken();
                    if( val != null && !val.trim().isEmpty() ) {
                        condidateip.add(val.trim());
                    }

                }
                if( condidateip.size() > 0) {
                    Random rd = new Random();
                    int num = rd.nextInt(condidateip.size());
                    mqttBrokerAddres = condidateip.get((num % condidateip.size()));
                }

                if( mqttBrokerAddres.startsWith("ssl")) {
                    isUsingSSL = true;
                }

                String hostName = Constants.HOST_NAME;
                StringBuffer sb = new StringBuffer();
                sb.append("_");
                sb.append("gateserver");
                sb.append("_");
                sb.append(hostName);
                MqttAsyncClient client = new MqttAsyncClient(
//                        "tcp://127.0.0.1:1883", //URI
//                        "ssl://127.0.0.1:8883",
                        mqttBrokerAddres,
                        MqttClient.generateClientId() + sb.toString(),//ClientId
                        new MemoryPersistence());

                lockClient.lock();
                mClient = client;
                lockClient.unlock();

                mClient.setCallback(new MqttCallback() {
                    @Override
                    public void connectionLost(Throwable throwable) {
                        try {
                            SystemMonitor.mqtt_cluster_topic.set(0L);
                            messageRecvingQueue.putFirst(new DataHolder(3));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                    @Override
                    public void messageArrived(String s, MqttMessage mqttMessage) throws Exception {

//                        DataHolder data = new DataHolder(s, mqttMessage);
//                        messageRecvingQueue.put(data);
                        deleveryMessage(s, mqttMessage.getPayload(), null);
                    }

                    @Override
                    public void deliveryComplete(IMqttDeliveryToken iMqttDeliveryToken) {

                    }
                });
            }
        }
        catch (MqttException ex ) {
            ex.printStackTrace();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }

        IMqttToken token = null;
        try {
            if (mClient != null) {
                MqttConnectOptions options = new MqttConnectOptions();
//                options.setUserName("");
//                String pwd = "";
//                char[] pwdBuf = new char[pwd.length()];
//                pwd.getChars(0, pwd.length(), pwdBuf, 0);
//                options.setPassword(pwdBuf);
                options.setKeepAliveInterval(60 * 2);
                token = mClient.connect(options);
                token.waitForCompletion(5000);
                if(token.isComplete() ) {
                    logger.info("MQTT connectted to Server " + mClient.getServerURI());
                    isConnectSuccess = true;
                }
                else {
                    logger.error("MQTT Server Connect Failed (timeout)!!!");
                }

            }
        }
        catch (MqttException ex ) {
            logger.error("MQTT Server Connect Failed!!!");
            ex.printStackTrace();
        }
        catch (Exception ex ) {
            ex.printStackTrace();
        }

//        if ( mClient.isConnected() ) {
//
//            try {
//                token = mClient.subscribe(new String[] {"s/" + Constants.HOST_NAME.toLowerCase() + "/#"}, new int[] {0});
//
//                token.waitForCompletion(5*1000);
//
//                if(token.isComplete()) {
//                    isSub1 = true;
//                    isSub2 = true;
//                }
//                else {
//                    logger.error("MQTT Server subscrible Failed (timeout)!!!");
//                }
//
//                if( true ) {
//                    for(Map.Entry<String,ConcurrentSkipListSet<MqttSession>> item : subscribledTopics.entrySet()) {
//                        token = mClient.subscribe(item.getKey(),0);
//                        if(!token.isComplete()) {
//                            logger.error("MQTT Server subscrible Failed (timeout)!!!");
//                        }
//                    }
//                }
//
//            } catch (MqttException e) {
//                logger.error("MQTT Server subscrible Failed!!!");
//                e.printStackTrace();
//            } catch (Exception e) {
//
//            }
//        }

        if( isConnectSuccess ) {
            isSub1 = isSub2 = true;
            final MqttAsyncClient currentClient = mClient;
            TopicStore.queryDumpAndMonitorTopics(new TopicStore.Topiclistener() {
                @Override
                public void topicSubEvent(String topic) {

                    SystemMonitor.objDebuger1 = currentClient;
                    SystemMonitor.objDebuger2 = mClient;
//                    logger.info("cluster try sub topic {}", topic);
                    if( currentClient == mClient && mClient.isConnected()) {
                        if( currentClient == mClient && mClient.isConnected()) {
                            try {
//                                logger.info("cluster sub topic {}", topic);
                                IMqttToken token = currentClient.subscribe(topic, 0);
                                SystemMonitor.mqtt_cluster_topic.incrementAndGet();
//                                token.waitForCompletion();
//                                if(token.isComplete()) {
//                                }
                            } catch (MqttException e) {
                                logger.error("TopicStore Sub error {}",e.toString(), e);
                                e.printStackTrace();
                            }
                        }
                    }
                }

                @Override
                public void topicUnsubEvent(String topic) {
//                    logger.info("cluster try unsub topic {}", topic);
                    if( currentClient == mClient && mClient.isConnected()) {
                        try {
//                            logger.info("cluster unsub topic {}", topic);
                            SystemMonitor.mqtt_cluster_topic.decrementAndGet();
                            IMqttToken token = currentClient.unsubscribe(topic);
//                            token.waitForCompletion();
//                            if(token.isComplete()) {
//                            }
                        } catch (MqttException e) {
                            logger.error("TopicStore Unsub error {}",e.toString(), e);
                            e.printStackTrace();
                        }
                    }
                }

                @Override
                public void dumpSubTopics(String[] topics) {
                    if( topics == null || topics.length == 0) {
                        return;
                    }
//                    logger.info("cluster try dumpsub topic {}", topics.length);
                    if( currentClient == mClient && mClient.isConnected()) {
                        try {
                            int[] qoses = new int[topics.length];
//                            logger.info("cluster dumpsub topic {}", topics.length);
                            SystemMonitor.mqtt_cluster_topic.addAndGet(topics.length);
                            IMqttToken token = currentClient.subscribe(topics, qoses);
//                            token.waitForCompletion();
//                            if(token.isComplete()) {
//                            }
                        } catch (MqttException e) {
                            logger.error("TopicStore dumpSub error {}",e.toString(), e);
                            e.printStackTrace();
                        }
                    }
                }

                @Override
                public void pubTopics(String topic, byte[] data) {
//                    logger.info("cluster rece topic {} {}", topic, data.length);
                    if( currentClient == mClient && mClient.isConnected()) {
                        try {
                            currentClient.publish(topic,data,0,false);
//                            token.waitForCompletion();
//                            if(token.isComplete()) {
//                            }
                        } catch (MqttException e) {
                            e.printStackTrace();
                        }
                    }
                    else {
                        deleveryMessage(topic, data, null);
                    }
                }

            });

//            TopicStore.setlistener(new TopicStore.Topiclistener() {
//                @Override
//                public void topicSubEvent(String topic, long allcount) {
//                    if( mClient != null && mClient.isConnected()) {
//                        try {
//                            IMqttToken token = mClient.subscribe(topic,0);
//                            token.waitForCompletion();
//                        } catch (MqttException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//
//                @Override
//                public void topicUnsubEvent(String topic, long allcount) {
//                    if( mClient != null && mClient.isConnected()) {
//                        try {
//                            IMqttToken token = mClient.unsubscribe(topic);
//                            token.waitForCompletion();
//                        } catch (MqttException e) {
//                            e.printStackTrace();
//                        }
//                    }
//                }
//            });
//
//            Map<String, AtomicLong> allSubTopics = TopicStore.dumpTopic();
//            String[] topics = new String[allSubTopics.size()];
//            int[] qoses = new int[allSubTopics.size()];
//            int i = 0;
//            for(Map.Entry<String,AtomicLong> item : allSubTopics.entrySet()) {
//                topics[i] = item.getKey();
//                qoses[i] = 0;
//                i++;
//            }
//
//            if( mClient != null && mClient.isConnected()) {
//                try {
//                    token = mClient.subscribe(topics, qoses);
//                    token.waitForCompletion();
//                    if(token.isComplete()) {
//                        isSub1 = isSub2 = true;
//                    }
//                } catch (MqttException e) {
//                    e.printStackTrace();
//                }
//            }

        }

        if(isConnectSuccess && isSub1 && isSub2 ) {
            logger.info("MQTT Connect and sub Success");
            hasConnectborker = 2;
        }
        else {
            logger.info("MQTT Connect and sub Failed");
            hasConnectborker = 3;
        }
    }


    public void publicMessage(String topic, byte[] data, MqttSession srcSession,  int version_code) {
        if( isServer ) {
            deleveryMessage(topic, data, srcSession);
        }
        else {
//            try {
//                DataHolder dh = new DataHolder(topic,data);
//                messageSendingQueue.put(dh);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }

            deleveryMessage(topic,data,srcSession);
            TopicStore.publishTopicData(topic,data);
        }

    }

    public void ProcessMessageQueue() {
        //发送消息
        mSendingWorkingExecutor.execute(new Runnable() {
            @Override
            public void run() {
                logger.debug("mSendingWorkingExecutor begin");
                while (!exitWorker) {
                    try {
                        DataHolder dh = messageSendingQueue.poll(3, TimeUnit.SECONDS);
                        MqttAsyncClient clientTemp = null;
                        int i = 0;
                        while (true) {
                            lockClient.lock();
                            clientTemp = mClient;
                            lockClient.unlock();
                            //连接订阅成功
                            if( clientTemp != null && clientTemp.isConnected() && hasConnectborker == 2) {
                                break;
                            }
                            if( exitWorker) {
                                break;
                            }
                            //连接订阅失败
                            if( hasConnectborker == 3 || (clientTemp != null && !clientTemp.isConnected())) {
                                try {
                                    //开始断线重连
                                    DataHolder dhFirst = messageRecvingQueue.peekFirst();
                                    if( dhFirst != null && dhFirst.type == 3) {
                                        // 如果队列里面已经有重连消息，就不用在放了。
                                    }
                                    else {
                                        messageRecvingQueue.putFirst(new DataHolder(3));
                                    }

                                    Thread.sleep(50);

                                } catch (Exception e) {
                                }
                                i++;
                            }
                            else {
                                if( i > 100 ) {
                                    //重连时间太长，报错。
                                    break;
                                }
                            }
                        }
                        // 心跳消息，如果连接上就发送,每30秒未发送就主动发送一下。
                        if( dh != null && dh.type == 2 ) {
                            if (lastSendTime == -1 || System.currentTimeMillis() - lastSendTime > 30 * 1000) {
                                if (clientTemp != null && clientTemp.isConnected()) {
                                    try {
                                        clientTemp.publish(dh.topic, dh.data, 0, false);
                                    } catch (MqttException e) {

                                    }
                                    lastSendTime = System.currentTimeMillis();
                                }
                            }
                        }
                        else if( dh != null && dh.type == 1) {
                            if( clientTemp != null && clientTemp.isConnected() ) {
                                try {
                                    clientTemp.publish(dh.topic, dh.data,0,false);
                                } catch (MqttException e) {
                                    e.printStackTrace();
                                }
                                lastSendTime = System.currentTimeMillis();
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                logger.debug("mSendingWorkingExecutor done");
            }
        });
        //接收消息
        mRecvingWorkingExecutor.execute(new Runnable() {
            @Override
            public void run() {
                logger.debug("mRecvingWorkingExecutor begin");
                int retryConnectTime = 1;
                while (!exitWorker) {
                    try {
                        DataHolder dh = messageRecvingQueue.poll(3, TimeUnit.SECONDS);
                        // 检测是否连接并尝试重连
                        if (mClient != null) {
                            //订阅失败或者连接失败
                            if (hasConnectborker == 3 || !mClient.isConnected()) {
                                try{
                                    mClient.setCallback(null);
                                } catch (Exception e) {

                                }
                                try {
                                    mClient.disconnectForcibly(3*1000,3*1000);
                                } catch (Exception e) {

                                }
                                try {

                                    mClient.close();
                                } catch (Exception e) {
                                }
                                lockClient.lock();
                                mClient = null;
                                lockClient.unlock();
                                tryConnectAndSubScrible();
                            }
                        } else {
                            tryConnectAndSubScrible();
                            if(exitWorker) continue;
                        }
                        //重连失败，等待继续重连
                        if (mClient != null && !mClient.isConnected()) {
                            try {
                                retryConnectTime *= 2;
                                if( retryConnectTime > 100 ) {
                                    retryConnectTime = 100;
                                }
                                Thread.sleep(100 * retryConnectTime);
                            } catch (Exception e) {

                            }
                            messageRecvingQueue.putFirst(dh);
                            continue;
                        }

                        retryConnectTime = 1;


                        if (dh != null && dh.type == 0) { //收到的消息
                            if( false && dh.topic.startsWith("$SYS")) {
//                                String data = new String(dh.message.getPayload(), Charset.forName("utf8"));
//                                PushRequestFactory.getInstance().SendToUsers(data);
                            }
                            else {

                                deleveryMessage(dh.topic,dh.message.getPayload(),null);
//                                Collection<MqttSession> sessions = getSessions(dh.topic);
//                                for( MqttSession session : sessions) {
//                                    session.sendData(dh.topic, dh.message.getPayload());
//                                }
//                                MqttWildcardTopicManager.getInstance().PublishMessage(dh.topic,dh.message.getPayload());
                            }


//                            //创建玩家session
//                            AbstractMQTTSession session = (AbstractMQTTSession) COKServer.getInstance().getSessionManager().getMqttSession(dh.topic);
//                            if (session == null) {
//
//                                MQTTCoreService engine = (MQTTCoreService) COKServer.getInstance().getCoreServiceByType(COKCoreServiceType.MQTT);
//                                MQTTSessionManager sessionManager = engine.getSessionManager();
//                                session = sessionManager.create(dh.topic, MQTTClientWorker.this);
//                                if (dh.clientid != null && !dh.clientid.isEmpty()) {
//                                    session.setAddress(client_ip.get(dh.clientid));
//                                }
//                                logger.info("create new mqtt session info:" + dh.topic);
//                                if (MQTTClientWorker.this.isUsingSSL) {
//                                    session.setProperty("SSLEncrypted", new Boolean(true));
//                                }
//                                COKServer.getInstance().getSessionManager().addMqttSession(session);
//                            }
//
//                            session.inputReliable(dh.message.getPayload());

                        }
                    } catch (InterruptedException e) {
                        exitWorker = true;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                }

                logger.debug("mRecvingWorkingExecutor done");
            }
        });
    }

    public void stopClient() {
        exitWorker = true;

//        List<Runnable> worker = mRecvingWorkingExecutor.shutdownNow();
//        worker = mSendingWorkingExecutor.shutdownNow();
        mHeartBeatFuture.cancel(true);
        if( mClient != null &&  mClient.isConnected()) {
            try {
                mClient.disconnect();
            } catch (MqttException e) {
//                e.printStackTrace();
            }
        }

        messageRecvingQueue.clear();
        messageSendingQueue.clear();
        Future<?> submit = mRecvingWorkingExecutor.submit(new Runnable() {
            @Override
            public void run() {

            }
        });
        try {
            submit.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        submit = mSendingWorkingExecutor.submit(new Runnable() {
            @Override
            public void run() {

            }
        });
        try {
            submit.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        SystemMonitor.mqtt_cluster_topic.set(0L);
        hasConnectborker = 0;
    }

    public void deleveryMessage(String topic, byte[] data, MqttSession srcSession) {
        Collection<MqttSession> sessions = getSessions(topic);
//        for( MqttSession session : sessions) {
//            session.sendData(topic, data);
//        }
//        MqttWildcardTopicManager.getInstance().PublishMessage(topic,data);
        Collection<MqttSession> sessions1 = MqttWildcardTopicManager.getInstance().getSessions(topic);
        HashSet<MqttSession> finalSessions = new HashSet<>();
        finalSessions.addAll(sessions);
        finalSessions.addAll(sessions1);

        ArrayList<MqttSession> sharedSession = new ArrayList<>();

        for( MqttSession session : finalSessions) {
            if ( session != null && session.getSessionType() == 3) {
                sharedSession.add(session);
                continue;
            }

            if( session != null && session == srcSession && session.getConectPort() == MqttServer.CLUSTER_Port ) {
                // cluster client
                continue;
            }

            session.sendData(topic, data);
        }
//        if (sharedSession.size() > 0 ) {
//            long num = sharedSession.size();
//            if( srcSession == null ) {
//                long index = CommonUtils.getRandomNum(num);
//                if( sharedSession.get((int)index) != null) {
//                    sharedSession.get((int)index).sendData(topic,data);
//                }
//            }
//
//            long stamp = srcSession.getTimestamp();
//            long index = stamp % num;
//            if( sharedSession.get((int)index) != null) {
//                sharedSession.get((int)index).sendData(topic,data);
//            }
//        }

    }
}