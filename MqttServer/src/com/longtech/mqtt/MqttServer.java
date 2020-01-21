package com.longtech.mqtt;

import com.longtech.mqtt.BL.LogicChecker;
import com.longtech.mqtt.Utils.CommonUtils;
import com.longtech.mqtt.Utils.Constants;
import com.longtech.mqtt.Utils.DiskUtilMap;
import com.longtech.mqtt.Utils.JWTUtil;
import com.longtech.mqtt.cluster.NodeManager;
import com.longtech.mqtt.cluster.TopicManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerSocketChannel;
import io.netty.channel.kqueue.KQueue;
import io.netty.channel.kqueue.KQueueEventLoopGroup;
import io.netty.channel.kqueue.KQueueServerSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kaiguo on 2018/12/12.
 */
public class MqttServer {

    static int TCP_Port = 0;
    static int SSL_Port = 0;
    static int WS_Port = 0;
    static int WSS_Port = 0;
    static int CTL_Port = 0;
    static int CLUSTER_Port = 0;
    static int CTL_SSL_Port = 0;

    public static void main(String[] args) {

        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);

        if ( args.length >= 1) {
            CommonUtils.filename = args[0];
        }

//        boolean res = JWTUtil.checkPassword("hello","eyJ0eXBlIjoiSldUIiwiYWxnIjoiSFMyNTYifQ.eyJuYW1lIjoiYm9iIiwgImFnZSI6NTAsInNhbHQiOjE4NDk4NDc1MjR9.vBx9rFftMuzxwQkEuDgU2ejxDnFKOpbWFYeIAJh9nO0");
//        ConcurrentHashMap<String, String> testRet = null;
//        DiskUtilMap.put("keya", "val1");
//        testRet = DiskUtilMap.getDiskMap();
//        for( Map.Entry<String,String> item: testRet.entrySet()) {
//            System.out.println(item.getKey() + " " + item.getValue());
//        }
//
//
//        DiskUtilMap.put("keyb", "val2");
//        testRet = DiskUtilMap.getDiskMap();
//        for( Map.Entry<String,String> item: testRet.entrySet()) {
//            System.out.println(item.getKey() + " " + item.getValue());
//        }
//
//        DiskUtilMap.put("keyc", "val3");
//        testRet = DiskUtilMap.getDiskMap();
//        for( Map.Entry<String,String> item: testRet.entrySet()) {
//            System.out.println(item.getKey() + " " + item.getValue());
//        }
//
//        DiskUtilMap.put("keyd", "val4");
//        testRet = DiskUtilMap.getDiskMap();
//        for( Map.Entry<String,String> item: testRet.entrySet()) {
//            System.out.println(item.getKey() + " " + item.getValue());
//        }
//
//        DiskUtilMap.put("keye", "val5");
//        testRet = DiskUtilMap.getDiskMap();
//        for( Map.Entry<String,String> item: testRet.entrySet()) {
//            System.out.println(item.getKey() + " " + item.getValue());
//        }
//
//
//        DiskUtilMap.remove("keya");
//        DiskUtilMap.remove("keyb");
//        DiskUtilMap.remove("keyc");
//        DiskUtilMap.remove("keyd");
//        DiskUtilMap.remove("keye");





//        int PORT = CommonUtils.getIntValue("server_port", 1883);
//        int PORT_IPV6 = CommonUtils.getIntValue("server_port_ipv6", 1883);
//        String ADDRESS_IPV6 = CommonUtils.getValue("server_address_ipv6","::");

        TCP_Port = CommonUtils.getIntValue("tcp_port",1883);
        SSL_Port = CommonUtils.getIntValue("ssl_port",1884);
        WS_Port = CommonUtils.getIntValue("ws_port", 8083);
        WSS_Port = CommonUtils.getIntValue("wss_port",8084);
        CTL_Port = CommonUtils.getIntValue("controller_port",18083);
        CTL_SSL_Port = CommonUtils.getIntValue("controller_ssl_port",18084);
        CLUSTER_Port = CommonUtils.getIntValue("cluster_port",18383);

        boolean SSL = false;
        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        if(Epoll.isAvailable()) {
            bossGroup = new EpollEventLoopGroup();
            workerGroup = new EpollEventLoopGroup();
        }
        else if(KQueue.isAvailable()) {
            bossGroup = new KQueueEventLoopGroup();
            workerGroup = new KQueueEventLoopGroup();
        }

        try
        {
            Constants.init();
            MqttSessionManager.init();
            MqttClientWorker.init();
            SystemMonitor.init();
            MqttWildcardTopicManager.init();
            MqttSessionManager.recoveryLastRunData(MqttClientWorker.getInstance());
            TopicManager.init();
            NodeManager.init();
            LogicChecker.init();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }


        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup);
//                    .option(ChannelOption.SO_BACKLOG,1024)
//                    .option(ChannelOption.MAX_MESSAGES_PER_READ,Integer.MAX_VALUE)
//                    .childOption(ChannelOption.ALLOCATOR,new PooledByteBufAllocator(true))
//                    .childOption(ChannelOption.SO_REUSEADDR,true)
//                    .childOption(ChannelOption.MAX_MESSAGES_PER_READ,Integer.MAX_VALUE);
            if( Epoll.isAvailable()) {
                b.channel(EpollServerSocketChannel.class);
            }
            else if(KQueue.isAvailable()) {
                b.channel(KQueueServerSocketChannel.class);
            }
            else {
                b.channel(NioServerSocketChannel.class);
            }

//            b.handler(new LoggingHandler(LogLevel.INFO));
//            b.childHandler(new WebSocketServerInitializer(null));
            SslContext sslCtx = null;
            try {
                sslCtx = SslContextBuilder.forServer(new File(Constants.CERT_FILE), new File(Constants.KEY_FILE)).build();
            } catch (Exception e){
                sslCtx = null;
            }

            b.childHandler(new MqttServerInitializer(sslCtx));
//            Channel ch = b.bind(1883).sync().channel();
//            Channel ch1 = b.bind(1884).sync().channel();
            List<ChannelFuture> futures = new ArrayList<>();
            futures.add(b.bind(TCP_Port).sync());
            futures.add(b.bind(SSL_Port).sync());
            futures.add(b.bind(WS_Port).sync());
            futures.add(b.bind(WSS_Port).sync());
            futures.add(b.bind(CTL_Port).sync());
            futures.add(b.bind(CTL_SSL_Port).sync());
            futures.add(b.bind(CLUSTER_Port).sync());
            for (ChannelFuture f: futures) {
                f.sync();
            }


//            Channel ch1 = null;
//            if( PORT_IPV6 != PORT ) {
//                ch1 = b.bind(ADDRESS_IPV6, PORT_IPV6).sync().channel();
//            }

            System.out.println("System start success at port:" + TCP_Port + " " + SSL_Port + " " + WS_Port + " " + WSS_Port + " " + CTL_Port + " " + CLUSTER_Port);

            for (ChannelFuture f: futures) {
                f.channel().closeFuture().sync();
            }
//            ch.closeFuture().sync();
//            if( ch1 != null ) {
//                ch1.closeFuture().sync();
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

}
