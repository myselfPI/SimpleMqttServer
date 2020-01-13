package com.longtech.mqtt;

import com.longtech.mqtt.Utils.Constants;
import com.longtech.mqtt.codec.ByteBufToWebSocketFrameEncoder;
import com.longtech.mqtt.codec.MqttDecoder;
import com.longtech.mqtt.codec.WebSocketFrameToByteBufDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.websocketx.WebSocketServerProtocolHandler;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Created by kaiguo on 2018/12/14.
 */
public class MqttHttpPortUnificationServer extends ByteToMessageDecoder {
    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        // Will use the first five bytes to detect a protocol.
        if (in.readableBytes() < 5) {
            return;
        }

        final int magic1 = in.getUnsignedByte(in.readerIndex());
        final int magic2 = in.getUnsignedByte(in.readerIndex() + 1);
        if (isHttp(magic1, magic2)) {
            switchToHttp(ctx);
        }
        else {
            switchToMqtt(ctx);
        }
    }

    private static boolean isHttp(int magic1, int magic2) {
        return
                magic1 == 'G' && magic2 == 'E' || // GET
                        magic1 == 'P' && magic2 == 'O' || // POST
                        magic1 == 'P' && magic2 == 'U' || // PUT
                        magic1 == 'H' && magic2 == 'E' || // HEAD
                        magic1 == 'O' && magic2 == 'P' || // OPTIONS
                        magic1 == 'P' && magic2 == 'A' || // PATCH
                        magic1 == 'D' && magic2 == 'E' || // DELETE
                        magic1 == 'T' && magic2 == 'R' || // TRACE
                        magic1 == 'C' && magic2 == 'O';   // CONNECT
    }
    private static final String MQTT_SUBPROTOCOL_CSV_LIST = "mqtt, mqttv3.1, mqttv3.1.1";
    private void switchToHttp(ChannelHandlerContext ctx) {
        ChannelPipeline pipeline = ctx.pipeline();
//        pipeline.addLast(new HttpServerCodec());
////        pipeline.addLast(new HttpObjectAggregator(65536));
//        pipeline.addLast(new HttpObjectAggregator(10 * 1024 * 1024));
//        pipeline.addLast(new HttpPageHandler());
//        pipeline.remove(this);



        pipeline.addLast(new HttpServerCodec())
                .addLast("aggregator", new HttpObjectAggregator(10 * 1024 * 1024))
                .addLast("pageHandler", new HttpPageHandler())
                .addLast("webSocketHandler",
                        new WebSocketServerProtocolHandler("/mqtt", MQTT_SUBPROTOCOL_CSV_LIST))
                .addLast("ws2bytebufDecoder", new WebSocketFrameToByteBufDecoder())
                .addLast("bytebuf2wsEncoder", new ByteBufToWebSocketFrameEncoder())
//                .addFirst("idleStateHandler", new IdleStateHandler(60, 0, 0))//MQTT协议中接入请求会带 Keep Alive。这里只是临时设置
//                .addAfter("idleStateHandler", "idleEventHandler", new IdleStateHandler(60, 0, 0, TimeUnit.SECONDS))
                .addLast("decoder", new MqttDecoder(20 * 1024*1024))
                .addLast("encoder", MqttEncoder.INSTANCE)
                .addLast("timeout", new IdleStateHandler(Constants.CLIENT_TIMEOUT, 0, 0, TimeUnit.SECONDS))
                .addLast(SERVER_HANDLER);
        pipeline.remove(this);
        pipeline.remove("timeoutAbsolute");
//                .addLast("messageLogger", MQTTLogHandler.INSTANCE)
//                .addLast("handler", this.mqttHandler);


    }
    private static final MqttServerHandler SERVER_HANDLER = new MqttServerHandler();
    private void switchToMqtt(final ChannelHandlerContext ctx) {
        ChannelPipeline pipeline = ctx.pipeline();
//        pipeline.addLast(new LoggingHandler(LogLevel.DEBUG));
        pipeline.addLast(new MqttDecoder(20 * 1024*1024));
        pipeline.addLast(MqttEncoder.INSTANCE);
        pipeline.addLast("timeout", new IdleStateHandler(Constants.CLIENT_TIMEOUT, 0, 0, TimeUnit.SECONDS));
        pipeline.addLast(SERVER_HANDLER);
        pipeline.remove(this);
        pipeline.remove("timeoutAbsolute");
        ctx.executor().schedule(new Runnable() {
            @Override
            public void run() {
                if( ctx.channel().isActive()) {
                    if( !ctx.channel().hasAttr(MqttServerHandler.USER)) {
                        ctx.channel().writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                    }
                }
            }
        },30,TimeUnit.SECONDS);
    }

    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception
    {
        if (evt instanceof IdleStateEvent)
        {
            IdleStateEvent event = (IdleStateEvent)evt;
            ChannelPipeline pipeline = ctx.pipeline();
            if (event.state().equals(IdleState.WRITER_IDLE)  )
            {
                if( ctx.channel().isActive()) {
                    ctx.channel().writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                }
            }
        }
        super.userEventTriggered(ctx, evt);
    }

}
