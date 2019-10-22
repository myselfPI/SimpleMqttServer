package com.longtech.mqtt;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.longtech.mqtt.Utils.CommonUtils;
import com.longtech.mqtt.cluster.NodeManager;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import static io.netty.handler.codec.http.HttpHeaders.Names.CONTENT_TYPE;
import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * Created by kaiguo on 2018/12/14.
 */
public class HttpPageHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    private boolean isDisable = false;

    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        super.channelRead(ctx,msg);
        if( isDisable ) {
            ctx.pipeline().remove(this);
        }

    }

    public boolean acceptInboundMessage(Object msg) throws Exception {
        FullHttpRequest req = (FullHttpRequest)msg;
        if(req != null && req.uri().contains("/mqtt")) {
            isDisable = true;
            return false;
        }
        return true;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) throws Exception {


        // Handle a bad request.
        if (!req.decoderResult().isSuccess()) {
            sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST));
            return;
        }
        long start = System.currentTimeMillis();
        process(req, ctx);
        long end = System.currentTimeMillis();
        if( end - start > 50) {
            logger.info("HTTP call [SLOW QUERY] {} {}", end - start,  req.uri());
        }
    }


    protected static final Logger logger = LoggerFactory.getLogger(HttpPageHandler.class);

    public static ByteBuf getContent(String webSocketLocation) {
        return null;
    }

    private static void sendHttpResponse(ChannelHandlerContext ctx, HttpRequest req, FullHttpResponse res,String result) {
        // Generate an error page if response getStatus code is not OK (200).
        if (res.status().code() != 200) {
            ByteBuf buf = Unpooled.copiedBuffer(res.status().toString(), CharsetUtil.UTF_8);
            res.content().writeBytes(buf);
            buf.release();
            HttpUtil.setContentLength(res, res.content().readableBytes());
        }

        logger.info("HTTP call END {} {} {}", req.uri(), res.status().code(), result);

        // Send the response and close the connection if necessary.
        ChannelFuture f = ctx.channel().writeAndFlush(res);
        if (!HttpUtil.isKeepAlive(req) || res.status().code() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static void sendHttpResponse(ChannelHandlerContext ctx, FullHttpRequest req, FullHttpResponse res) {
        // Generate an error page if response getStatus code is not OK (200).
        if (res.status().code() != 200) {
            ByteBuf buf = Unpooled.copiedBuffer(res.status().toString(), CharsetUtil.UTF_8);
            res.content().writeBytes(buf);
            buf.release();
            HttpUtil.setContentLength(res, res.content().readableBytes());
        }

        // Send the response and close the connection if necessary.
        ChannelFuture f = ctx.channel().writeAndFlush(res);
        if (!HttpUtil.isKeepAlive(req) || res.status().code() != 200) {
            f.addListener(ChannelFutureListener.CLOSE);
        }
    }

    private static void failedReturn(ChannelHandlerContext ctx, HttpRequest req, String reason) {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("code", -1);
        jsonObj.put("message",reason);
        String result = CommonUtils.toJSONString(jsonObj);
        ByteBuf content = Unpooled.copiedBuffer(result, CharsetUtil.UTF_8);
        FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, OK, content);
        res.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");
        HttpUtil.setContentLength(res, res.content().readableBytes());
        sendHttpResponse(ctx, req, res, result);
    }

    private static void successReturn(ChannelHandlerContext ctx, HttpRequest req) {
        successReturn(ctx, req, null);
    }
    private static void successReturn(ChannelHandlerContext ctx, HttpRequest req, Object obj) {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("code", 1);
        jsonObj.put("message","success");
        jsonObj.put("data", obj);
        String result = CommonUtils.toJSONString(jsonObj);
        ByteBuf content = Unpooled.copiedBuffer(result, CharsetUtil.UTF_8);
        FullHttpResponse res = new DefaultFullHttpResponse(HTTP_1_1, OK, content);
        res.headers().set(CONTENT_TYPE, "application/json; charset=UTF-8");
        HttpUtil.setContentLength(res, res.content().readableBytes());
        sendHttpResponse(ctx, req, res, result);
    }


    public static void process(FullHttpRequest req, ChannelHandlerContext ctx ) {
        HttpHeaders headers = req.headers();
        try {
            URI uri = new URI(req.uri());
            Map<String,String> params = CommonUtils.MakePairs(uri.getQuery());
            String content = req.content().toString(StandardCharsets.UTF_8);
            Map<String, String> params_post = CommonUtils.MakePairs(content);

            params.putAll(params_post);

            String path = uri.getPath();
            if( path.startsWith("/")) {

                path = path.substring(1);
                if( path.endsWith("/")) {
                    path = path.substring(0, path.length() -1);
                }
                path = path.replace('/','_');
                if(StringUtil.isNullOrEmpty(path)) {
                    path = "system_status";
                }
            }
            try {
                logger.info("HTTP call BEGIN {} {}", req.uri(), CommonUtils.toJSONString(params));
                Method m = HttpPageHandler.class.getDeclaredMethod(path, HttpRequest.class, ChannelHandlerContext.class, Map.class);
                if( m == null) {
                }
                else {
                    m.invoke(null, req,ctx,params);
                }
            } catch ( NoSuchMethodException e ) {

//                logger.error("http restful {} not found", path);
                sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, NOT_FOUND),"no method");
            } catch ( SecurityException e) {
//                logger.error("http restful {} not found", path);
                sendHttpResponse(ctx, req, new DefaultFullHttpResponse(HTTP_1_1, NOT_FOUND),"no method");
            } catch (InvocationTargetException e) {
                throw (Exception)e.getCause();
            }



        } catch (Exception e) {
            logger.error("[WebPageHandler process] ERROR " + req.uri(), e);
        }

    }


    public static Map getJsonObj(ConcurrentHashMap<String, ConcurrentSkipListSet<MqttSession>> data) {
        JSONObject obj= new JSONObject();
        for(Map.Entry<String, ConcurrentSkipListSet<MqttSession>> item : data.entrySet()) {
            JSONArray array = new JSONArray();
            for( MqttSession valItem: item.getValue()) {
                array.add(valItem.toString());
            }

            obj.put(item.getKey(), array);

        }
        return obj;
    }

    public static Collection getJson(ConcurrentHashMap<Channel, MqttSession> data) {
        JSONArray obj= new JSONArray();
        for(Map.Entry<Channel, MqttSession> item : data.entrySet()) {
            obj.add(item.getValue().toString());
        }
        return obj;
    }

    public static void system_status( HttpRequest req, ChannelHandlerContext ctx, Map<String,String> params) {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("online", MqttSessionManager.getInstance().getCurrentSessions());
        jsonObj.put("connects", SystemMonitor.connectNumber.longValue());
        jsonObj.put("RPS", SystemMonitor.rps.longValue());
        jsonObj.put("allRequest", SystemMonitor.allReqCount.longValue());
        jsonObj.put("allTopics",MqttClientWorker.getInstance().getSubscribledTopicsSize());
        jsonObj.put("allWildcardTopics", MqttWildcardTopicManager.getInstance().getSimpleWildTopicSessionsSize());
        jsonObj.put("send_count", SystemMonitor.send_count.get());
        jsonObj.put("recv_count", SystemMonitor.recv_count.get());
        jsonObj.put("connect_count", SystemMonitor.connect_count);
        successReturn(ctx, req, jsonObj);
    }

    public static void system_status_detail( HttpRequest req, ChannelHandlerContext ctx, Map<String,String> params) {
        JSONObject jsonObj = new JSONObject();
        jsonObj.put("online", MqttSessionManager.getInstance().getCurrentSessions());
        jsonObj.put("connects", SystemMonitor.connectNumber.longValue());
        jsonObj.put("RPS", SystemMonitor.rps.longValue());
        jsonObj.put("allRequest", SystemMonitor.allReqCount.longValue());
        jsonObj.put("allTopics",MqttClientWorker.getInstance().getSubscribledTopicsSize());
        jsonObj.put("allWildcardTopics", MqttWildcardTopicManager.getInstance().getWildTopicSessionsSize());
        jsonObj.put("Sessions", getJson(MqttSessionManager.getInstance().getSessionMap()));
        jsonObj.put("MqttTopics", getJsonObj(MqttClientWorker.getInstance().getSubscribledTopics()));
        jsonObj.put("WildcardTopics", getJsonObj(MqttWildcardTopicManager.getInstance().getWildcardTopicSessions()));
        jsonObj.put("Name", NodeManager.getInstance().getMyNodeName());
        successReturn(ctx, req, jsonObj);
    }

}
