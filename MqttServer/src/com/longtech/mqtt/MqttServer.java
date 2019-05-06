package com.longtech.mqtt;

import com.longtech.mqtt.Utils.CommonUtils;
import com.longtech.mqtt.Utils.DiskUtilMap;
import com.longtech.mqtt.Utils.JWTUtil;
import com.longtech.mqtt.cluster.NodeManager;
import com.longtech.mqtt.cluster.TopicManager;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
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
import io.netty.util.internal.StringUtil;
import io.netty.util.internal.logging.InternalLoggerFactory;
import io.netty.util.internal.logging.Slf4JLoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kaiguo on 2018/12/12.
 */
public class MqttServer {

    public static void main(String[] args) {

        InternalLoggerFactory.setDefaultFactory(Slf4JLoggerFactory.INSTANCE);

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





        int PORT = CommonUtils.getIntValue("server_port", 1883);
        int PORT_IPV6 = CommonUtils.getIntValue("server_port_ipv6", 1883);
        String ADDRESS_IPV6 = CommonUtils.getValue("server_address_ipv6","::");

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
            MqttSessionManager.init();
            MqttClientWorker.init();
            SystemMonitor.init();
            MqttWildcardTopicManager.init();
            MqttSessionManager.recoveryLastRunData(MqttClientWorker.getInstance());
            TopicManager.init();
            NodeManager.init();
        }
        catch (Exception ex) {
            ex.printStackTrace();
        }


        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .option(ChannelOption.SO_BACKLOG,1024)
                    .option(ChannelOption.MAX_MESSAGES_PER_READ,Integer.MAX_VALUE)
                    .childOption(ChannelOption.ALLOCATOR,new PooledByteBufAllocator(true))
                    .childOption(ChannelOption.SO_REUSEADDR,true)
                    .childOption(ChannelOption.MAX_MESSAGES_PER_READ,Integer.MAX_VALUE);
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
            b.childHandler(new MqttServerInitializer(null));
            Channel ch = b.bind(PORT).sync().channel();

            Channel ch1 = null;
            if( PORT_IPV6 != PORT ) {
                ch1 = b.bind(ADDRESS_IPV6, PORT_IPV6).sync().channel();
            }

            System.out.println("System start success " +
                    (SSL? "https" : "http") + "://127.0.0.1:" + PORT + '/');

            ch.closeFuture().sync();
            if( ch1 != null ) {
                ch1.closeFuture().sync();
            }
        } catch (Exception e) {

        }
        finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

}
