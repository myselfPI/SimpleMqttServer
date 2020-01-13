package com.longtech.mqtt;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpServerCodec;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;

/**
 * Created by kaiguo on 2018/12/13.
 */
public class MqttServerInitializer extends ChannelInitializer<SocketChannel> {

    private static final MqttServerHandler SERVER_HANDLER = new MqttServerHandler();
    private final SslContext sslCtx;


    public MqttServerInitializer(SslContext sslCtx) {
        this.sslCtx = sslCtx;
    }

    @Override
    public void initChannel(SocketChannel ch) throws Exception
    {
        ChannelPipeline pipeline = ch.pipeline();
        if (sslCtx != null)
        {
            if( ch.localAddress().getPort() == MqttServer.SSL_Port || ch.localAddress().getPort()==MqttServer.WSS_Port
                    || ch.localAddress().getPort() == MqttServer.CTL_SSL_Port) {
                pipeline.addLast(sslCtx.newHandler(ch.alloc()));
            }
        }

        if( true || ch.localAddress().getPort()==1883)
        {/*
             //mqtt消息解码、编码器
            pipeline.addLast(new MqttDecoder(81920));
            pipeline.addLast("timeout", new IdleStateHandler(0, 0, 20, TimeUnit.SECONDS));
            //自定义mqtt消息业务处理
            pipeline.addLast(SERVER_HANDLER);
            pipeline.addLast(MqttEncoder.INSTANCE);
        */
            pipeline.addLast("timeoutAbsolute", new IdleStateHandler(0, 20, 0, TimeUnit.SECONDS));
            pipeline.addLast(new MqttHttpPortUnificationServer());
//            pipeline.addLast(new MqttDecoder(20 * 1024*1024));
//            pipeline.addLast(MqttEncoder.INSTANCE);
//
//            pipeline.addLast("timeout", new IdleStateHandler(30, 0, 120, TimeUnit.SECONDS));
//            pipeline.addLast(SERVER_HANDLER);

//            new WebSocketFrameToByteBufDecoder();
        }
        else
        {
            //http请求消息解码、编码器，并将http 分段请求消息整合成 FullHttpRequest
            pipeline.addLast(new HttpRequestDecoder());
            pipeline.addLast(new HttpObjectAggregator(65536));
            pipeline.addLast(new HttpResponseEncoder());
            //自定义http请求消息业务处理
//            pipeline.addLast(new HttpServerHandler());

        }




    }

}
