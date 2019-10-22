package com.longtech.mqtt;

import com.longtech.mqtt.BL.LogicChecker;
import com.longtech.mqtt.Utils.JWTUtil;
import com.longtech.mqtt.cluster.MqttServerNodeSession;
import com.longtech.mqtt.cluster.NodeManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.EmptyByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.mqtt.*;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.AttributeKey;

import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
/**
 * Created by kaiguo on 2018/12/13.
 */
@ChannelHandler.Sharable
public class MqttServerHandler extends SimpleChannelInboundHandler<Object>
{

    public static Logger logger = LoggerFactory.getLogger(MqttServerHandler.class);

    private final AttributeKey<String> USER = AttributeKey.valueOf("user");

    public static MqttFixedHeader CONNACK_HEADER = new MqttFixedHeader(MqttMessageType.CONNACK, false,MqttQoS.AT_MOST_ONCE,false,0);
    public static MqttFixedHeader SUBACK_HEADER = new MqttFixedHeader(MqttMessageType.SUBACK, false,MqttQoS.AT_MOST_ONCE,false,0);
    public static MqttFixedHeader PUBACK_HEADER = new MqttFixedHeader(MqttMessageType.PUBACK, false,MqttQoS.AT_MOST_ONCE,false,0);
    public static MqttFixedHeader PUBLISH_HEADER = new MqttFixedHeader(MqttMessageType.PUBLISH, false,MqttQoS.AT_MOST_ONCE,false,0);
    public static MqttFixedHeader UNSUBACK_HEADER = new MqttFixedHeader(MqttMessageType.UNSUBACK, false,MqttQoS.AT_MOST_ONCE,false,0);
    public static MqttFixedHeader PINGRESP_HEADER = new MqttFixedHeader(MqttMessageType.PINGRESP, false,MqttQoS.AT_MOST_ONCE,false,0);
    public static MqttFixedHeader PUBREC_HEADER = new MqttFixedHeader(MqttMessageType.PUBREC, false,MqttQoS.AT_MOST_ONCE,false,0);


    @Override
    //连接成功后调用的方法
    public void channelActive(ChannelHandlerContext ctx) throws Exception
    {
        logger.debug("Socket Connect {}", (((SocketChannel) ctx.channel()).remoteAddress().getAddress().getHostAddress()));
    }

    @Override
    public void channelRead0(final ChannelHandlerContext ctx, final Object request) throws Exception
    {
        ReferenceCountUtil.retain(request);
        ctx.executor().execute(new Runnable() {
            @Override
            public void run() {

                try
                {
                    //处理mqtt消息
                    if (((MqttMessage)request).decoderResult().isSuccess())
                    {
                        SystemMonitor.reqCount.incrementAndGet();
                        SystemMonitor.allReqCount.incrementAndGet();
                        MqttMessage req = (MqttMessage)request;
                        logger.debug("Receved Mqtt {}", req.fixedHeader().messageType() );
                        SystemMonitor.recv_count.incrementAndGet();
                        switch (req.fixedHeader().messageType())
                        {
                            case CONNECT:
                                SystemMonitor.connect_count.incrementAndGet();
                                doConnectMessage(ctx, request);
                                return;
                            case SUBSCRIBE:
                                doSubMessage(ctx, request);
                                return;
                            case UNSUBSCRIBE:
                                doUnSubMessage(ctx,request);
                                return;
                            case PUBLISH:
                                doPublishMessage(ctx, request);
                                return;
                            case PINGREQ:
                                doPingreoMessage(ctx, request);
                                return;
                            case PUBACK:
                                doPubAck(ctx, request);
                                return;
                            case PUBREC:
                            case PUBREL:
                            case PUBCOMP:
                            case UNSUBACK:
                                return;
                            case PINGRESP:
                                doPingrespMessage(ctx, request);
                                return;
                            case DISCONNECT:
//                        ctx.close();
                                MqttSession session = MqttSessionManager.getInstance().getSession(ctx.channel());
                                session.setAbnormalExit(false);
                                ctx.channel().writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                                return;
                            default:
                                return;
                        }
                    }
                }
                catch (Exception ex)
                {
                    logger.error("ERROR", ex);
                }
                finally {
                    ReferenceCountUtil.release(request);
                }
            }
        });

    }

    @Override
    public void channelInactive(final ChannelHandlerContext ctx)
    {
        ctx.executor().execute(new Runnable() {
            @Override
            public void run() {
                //清理用户缓存
                if (ctx.channel().hasAttr(USER)) {
//            String user = ctx.channel().attr(USER).get();
//            userMap.remove(user);
//            userOnlineMap.remove(user);

                    MqttSession session = MqttSessionManager.getInstance().removeSession(ctx.channel());
                    if (session != null) {
                        SystemMonitor.connectNumber.decrementAndGet();
                        logger.debug("connectNumber remove {} {}", session.getSid(), SystemMonitor.connectNumber.get());
                        session.unSubAllTopics();
                        session.publicOfflineEvent();
                        session.publicWillMessage();
                        LogicChecker.getInstance().removeClientid(session.getClientid(), session);
                        logger.debug("Session {} has delete!", session.getClientid());

                    }

                }
            }
        });

    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception
    {
        if (evt instanceof IdleStateEvent)
        {
            IdleStateEvent event = (IdleStateEvent)evt;
            if (event.state().equals(IdleState.READER_IDLE))
            {
//                if (ctx.channel().hasAttr(USER)){
//                    String user = ctx.channel().attr(USER).get();
//                    log.debug("ctx heartbeat timeout,close!"+user);//+ctx);
//                    log.debug("ctx heartbeat timeout,close!");//+ctx);
//                    if(unconnectMap.containsKey(user))
//                    {
//                        unconnectMap.put(user, unconnectMap.get(user)+1);
//                    }else
//                    {
//                        unconnectMap.put(user, new Long(1));
//                    }
//                }
                logger.debug("Timeout Mqtt");
                if( ctx.channel().isActive()) {
                    ctx.channel().writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                }


//                buildHearBeat(ctx);
            }
//            else if(event.state().equals(IdleState.READER_IDLE))
//            {
////                log.debug("发送心跳给客户端！");
//                buildHearBeat(ctx);
//            }
        }
        super.userEventTriggered(ctx, evt);
    }

    private void doPingreoMessage(ChannelHandlerContext ctx, Object request)
    {
        //MqttMessage message=(MqttMessage)request;
//        System.out.println("响应心跳！");
//        buildHearBeat(ctx);
        MqttFixedHeader header = new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessage pingespMessage = new MqttMessage(header);
        ctx.writeAndFlush(pingespMessage);
        MqttSession session = MqttSessionManager.getInstance().getSession(ctx.channel());
        if( session != null ) {
            logger.debug("Session {} heat reo", session.getClientid());
        }

    }

    private void doPingrespMessage(ChannelHandlerContext ctx, Object request)
    {
        //       System.out.println("收到心跳请求！");
    }


    private void buildHearBeat(ChannelHandlerContext ctx)
    {
        MqttFixedHeader mqttFixedHeader=new MqttFixedHeader(MqttMessageType.PINGREQ, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessage message=new MqttMessage(mqttFixedHeader);
//        logger.debug("Receved Mqtt {}", req.fixedHeader().messageType() );
        logger.debug("Send Mqtt {}", MqttMessageType.PINGREQ );
        ctx.writeAndFlush(message);
    }

    public static MqttPublishMessage buildPublish(String str, String topicName, Integer messageId)
    {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, str.length());
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(topicName, messageId);//("MQIsdp",3,false,false,false,0,false,false,60);
        ByteBuf payload = Unpooled.wrappedBuffer(str.getBytes(CharsetUtil.UTF_8));
        MqttPublishMessage msg = new MqttPublishMessage(mqttFixedHeader, variableHeader, payload);
        logger.debug("Send Mqtt {}", MqttMessageType.PUBLISH );
        return msg;
    }

    public static MqttPublishMessage buildPublish(String topicName, byte[] datas, Integer messageId)
    {
        MqttFixedHeader mqttFixedHeader = new MqttFixedHeader(MqttMessageType.PUBLISH, false, MqttQoS.AT_MOST_ONCE, false, (int)datas.length);
        MqttPublishVariableHeader variableHeader = new MqttPublishVariableHeader(topicName, messageId);//("MQIsdp",3,false,false,false,0,false,false,60);
        ByteBuf payload = Unpooled.wrappedBuffer(datas);
        MqttPublishMessage msg = new MqttPublishMessage(mqttFixedHeader, variableHeader, payload);
        logger.debug("Send Mqtt {}", MqttMessageType.PUBLISH );
        return msg;
    }

    private void doConnectMessage(ChannelHandlerContext ctx, Object request)
    {
        MqttConnectMessage message = (MqttConnectMessage)request;

        String username = message.payload().userName();
        if( username == null ) {
            username = "";
        }
        String pwd = message.payload().password();
        // temp disable password check
        if( false && !JWTUtil.checkPassword(username,pwd)) {
            MqttConnAckVariableHeader variableheader = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD, false);
            MqttConnAckMessage connAckMessage = new MqttConnAckMessage(CONNACK_HEADER, variableheader);
            //ctx.write(MQEncoder.doEncode(ctx.alloc(),connAckMessage));
            ctx.writeAndFlush(connAckMessage).addListener(ChannelFutureListener.CLOSE);
            logger.debug("Send Mqtt {} {}", MqttMessageType.CONNACK, MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD);
            return;
        }
        MqttConnAckVariableHeader variableheader = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false);
        MqttConnAckMessage connAckMessage = new MqttConnAckMessage(CONNACK_HEADER, variableheader);
        //ctx.write(MQEncoder.doEncode(ctx.alloc(),connAckMessage));
        ctx.writeAndFlush(connAckMessage);
        logger.debug("Send Mqtt {} {}",  MqttMessageType.CONNACK, MqttConnectReturnCode.CONNECTION_ACCEPTED);
        ChannelPipeline pipeline = ctx.pipeline();
//        pipeline.remove("timeout");
        if( message.variableHeader().keepAliveTimeSeconds() > 0 ) {
//            pipeline.remove("timeout");
            pipeline.replace("timeout","timeout", new IdleStateHandler(message.variableHeader().keepAliveTimeSeconds() + message.variableHeader().keepAliveTimeSeconds()/2, 0, 0, TimeUnit.SECONDS));
        }


        //String user = message.variableHeader().name();
        String clientid = message.payload().clientIdentifier();
        logger.debug("Session {} create", clientid);
        //将用户信息写入变量

        if (!ctx.channel().hasAttr(USER))
        {
            ctx.channel().attr(USER).set(clientid);
        }
//        //将连接信息写入缓存
//        userMap.put(stb_code, ctx);
//        userOnlineMap.put(stb_code, DateUtil.getCurrentTimeStr());
////        log.debug("the user num is " + userMap.size());



        MqttSession session = null ;
        if( !username.equals("cluster") ) {
            session = new MqttSession();
        }
        else {
            session = new MqttServerNodeSession();
        }
        session.setClientid(clientid);
        session.setContext(ctx);
        session.setIpaddress((((SocketChannel) ctx.channel()).remoteAddress().getAddress().getHostAddress()));
        session.setPort((((SocketChannel) ctx.channel()).remoteAddress().getPort()));

        session.setWillTopic(message.payload().willTopic());
        session.setWillMessage(message.payload().willMessageInBytes());
        session.generateTimestamp();
        logger.debug("GK add session: {} {}", session.getSid(), session.getTimestamp());

//        MqttSession oldClientidSession = MqttSessionManager.getInstance().addSession(session, ctx.channel(),true);



        if( session.getSessionType() == 2 ) {
            MqttServerNodeSession serverSessoin = (MqttServerNodeSession)session;
        }


        session.publicOnlineEvent();
        SystemMonitor.connectNumber.incrementAndGet();
        logger.debug("connectNumber add {} {}", session.getSid(), SystemMonitor.connectNumber.get());
        MqttSessionManager.getInstance().addSession(session, ctx.channel());

//        MqttSessionManager.getInstance().getDuplicateClientIdSession(clientid);
//        if( oldClientidSession != null ) {
//            oldClientidSession.setAbnormalExit(false);
//            oldClientidSession.kick();
//
//        }

        if( session.getSessionType() == 1) {
            LogicChecker.getInstance().addClientid(clientid, session);
        }

    }

    private void doSubMessage(ChannelHandlerContext ctx, Object request)
    {
        MqttSubscribeMessage message = (MqttSubscribeMessage)request;
        int msgId = message.variableHeader().messageId();
        if (msgId == -1)
            msgId = 1;
        MqttMessageIdVariableHeader header = MqttMessageIdVariableHeader.from(msgId);
        MqttSubAckPayload payload = new MqttSubAckPayload(0);
        MqttSubAckMessage suback = new MqttSubAckMessage(SUBACK_HEADER, header, payload);
        ctx.writeAndFlush(suback);
        logger.debug("Send Mqtt {}", MqttMessageType.SUBACK);
        List<MqttTopicSubscription> topics = message.payload().topicSubscriptions();

        MqttSession session = MqttSessionManager.getInstance().getSession(ctx.channel());

        for( MqttTopicSubscription item : topics) {
//            MqttClientWorker.getInstance().subscribe(item.topicName(), session);

            if( session != null ) {
                logger.debug("Session {} sub {} id {}", session.getClientid(), item, msgId );
                session.subTopics(item.topicName());
            }
        }
    }

    private void doUnSubMessage(ChannelHandlerContext ctx, Object request)
    {
        MqttUnsubscribeMessage message = (MqttUnsubscribeMessage)request;
        int msgId = message.variableHeader().messageId();
        if (msgId == -1)
            msgId = 1;
        MqttMessageIdVariableHeader header = MqttMessageIdVariableHeader.from(msgId);
        MqttUnsubAckMessage unsubAckMessage = new MqttUnsubAckMessage(UNSUBACK_HEADER, header);
        ctx.writeAndFlush(unsubAckMessage);

        List<String> topics = message.payload().topics();

        MqttSession session = MqttSessionManager.getInstance().getSession(ctx.channel());
        for( String item : topics) {
//            MqttClientWorker.getInstance().subscribe(item.topicName(), session);
            if( session != null ) {
                session.unSubTopics(item);
            }
        }
    }

    private void doPubAck(ChannelHandlerContext ctx, Object request)
    {
        MqttPubAckMessage message = (MqttPubAckMessage)request;
//        log.debug(request);
        /* String user = ctx.channel().attr(USER).get();
         Map<String, UpMessage> requestMap=upMap.get(message.variableHeader().messageId());
         if(requestMap!=null&&requestMap.size()>0)
         {
             UpMessage upmessage=requestMap.get(user);
             if(upmessage!=null)
             {
                 upmessage.setStatus(Constants.SENDSUCESS);
                 requestMap.put(user, upmessage);
                 upMap.put(message.variableHeader().messageId(), requestMap);
             }
         }*/
    }

    private void doPublishMessage(ChannelHandlerContext ctx, Object request)
    {
        //        long time = System.currentTimeMillis();
        MqttPublishMessage message = (MqttPublishMessage)request;
        String topic = message.variableHeader().topicName();
        ByteBuf buf = message.payload();
        byte[] bytes = new byte[buf.readableBytes()];
        int readerIndex = buf.readerIndex();
        buf.getBytes(readerIndex, bytes);
        String msg = new String(ByteBufUtil.getBytes(buf));
        logger.debug("Session {} Recevied Client：{},{}",ctx.channel().attr(USER).get(), topic , bytes.length);
        int msgId = message.variableHeader().messageId();
        if (msgId == -1)
            msgId = 1;
        MqttSession session = MqttSessionManager.getInstance().getSession(ctx.channel());

        if( topic.equals("/cluster/addNode")) {
            NodeManager.getInstance().addNode(new String(bytes));
            if( session.getSessionType() == 2 ) {
                MqttServerNodeSession serverSession = (MqttServerNodeSession)session;
                serverSession.setNodeAddress(new String(bytes));
            }
            return;
        }

        if( topic.equals("/cluster/addOtherNode")) {
            NodeManager.getInstance().addNode(new String(bytes));
            return;
        }

        // cluster rpc call
        if( session.getSessionType() == 2 ) {
            MqttServerNodeSession serverSession = (MqttServerNodeSession)session;
            serverSession.receivedPublishedData(topic, bytes);
            return;
        }


//        if (message.fixedHeader().qosLevel() == MqttQoS.AT_MOST_ONCE)
//        {
//            MqttMessageIdVariableHeader header = MqttMessageIdVariableHeader.from(msgId);
//            MqttPubAckMessage puback = new MqttPubAckMessage(PUBACK_HEADER, header);
//            ctx.writeAndFlush(puback);
//            logger.debug("Send Mqtt {}", MqttMessageType.PUBACK);
//        }


        MqttClientWorker.getInstance().publicMessage(topic, bytes, session, 1);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause)
    {
        logger.error("ERROR exceptionCaught", cause);
        if( ctx.channel().isActive()) {
            ctx.channel().writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
        }
    }


//    public void sendMessage() {
//        MqttPublishMessage pubMsg;
//        UpMessage upmessage=new UpMessage();
//        upmessage.setDeviceId(stb_code);
//        upmessage.setMsgCode(msg.getMsgInfo().getMsgCode());
//        upmessage.setStatus(Constants.SENDSUCCESS);
//        upmessage.setDate(UpMessage.getCurrentDate());
//        upmessage.setUserNums(usernums);
//        upmessage.setMsgType(msg.getMsgInfo().getMsgType());
//        pubMsg = MQTTServerHandler.buildPublish(GsonJsonUtil.toJson(msg.getMsgInfo()), Constants.TOPIC_STB, 222);
////                        ReferenceCountUtil.retain(pubMsg);
//        ctxx.writeAndFlush(pubMsg);
//    }
//    public static void main(String[] args)
//    {
////        String msg = "{\"deviceNum\":\"88888888\",\"jumpFlag\":0,\"msgId\":\"M20170829153611748025\",\"status\":1,\"msgType\":6}";
//        String msg = "{\"deviceNum\":\"88888888\",\"jumpFlag\":0,\"msgId\":\"M20170829153611748025\",\"status\":1}";
//        StbReportMsg stbmsg= GsonJsonUtil.fromJson(msg, StbReportMsg.class);
//        System.out.println(stbmsg.getMsgType());
//    }
}