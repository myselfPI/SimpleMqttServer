package com.longtech.mqtt.BL;

import com.longtech.mqtt.MqttSession;
import com.longtech.mqtt.Utils.CommonUtils;
import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by kaiguo on 2020/5/18.
 */
public class ACLController {
    private static Logger logger = LoggerFactory.getLogger(ACLController.class);
    public static boolean IsAllowSub( MqttSession session, String topic) {

        if( true ) {
            return true;
        }

        String id = session.getClientid();
        String ip = session.getIpaddress();
        String user = session.getUser();
        String pwd = session.getPwd();


        if( AllowAnyTopicIp.containsKey(ip)) {
            return true;
        }
        int pos = ip.lastIndexOf(',');
        String partip = ip.substring(0, pos);
        if( AllowPartTopicIp.containsKey(partip)) {
            return true;
        }

        if( id == null ) {
            return false;
        }

        if( id.startsWith("android_") || id.startsWith("iOS_")) {
            String[] strs = id.split("\\_");
            String gameid = strs[1];
            String uid = strs[2];
            if ( topic.startsWith("elva/topic/" + gameid + "/" + uid)) {
                if ( !"#".equals(gameid) && !"?".equals(gameid) && !"#".equals(uid) && !"?".equals(uid)) {
                    return true;
                }
            }
        } else if ( id.startsWith("H5clientid_") || id.startsWith("BKDclientid_")) {
            String appid = user;
            if  ( topic.startsWith("elva/topic/") ) {

                String strs[] = topic.split("\\/");
                String gameid = strs[1];
                String uid = strs[2];
                if ( !"#".equals(gameid) && !"?".equals(gameid) && !"#".equals(uid) && !"?".equals(uid)) {
                    return true;
                } else {
                    return false;
                }

            } else if(topic.startsWith("mqtt/sendMessage")) {
                if ( topic.contains("#") || topic.contains("?")) {
                    return false;
                }
                else {
                    return true;
                }
            }
        } else if ( id.startsWith("smartviewId_")) {
            if ( topic.contains("#") || topic.contains("?")) {
                return false;
            }
            else {
                return true;
            }
        }

        if( user.equals("serveraihelp") && pwd.equals("pwdaihelp2020")) {
            return true;
        }

        if (AllowAnyUserPWD.size() > 0 ) {
            if (AllowAnyUserPWD.containsKey(user + ":" +pwd)) {
                return true;
            }
        }

        return false;
    }

    public static ConcurrentHashMap<String,Boolean> AllowAnyTopicIp = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String,Boolean> AllowAnyUserPWD = new ConcurrentHashMap<>();
    public static ConcurrentHashMap<String,Boolean> AllowPartTopicIp = new ConcurrentHashMap<>();


    public static boolean checkPassword(String user, String pwd, String clientid, String ip ) {
        if( AllowAnyTopicIp.containsKey(ip)) {
            return true;
        }

//        if( AllowPartTopicIp.contains(ip)) {
//            return true;
//        }
        if( user.equals("serveraihelp") && pwd.equals("pwdaihelp2020")) {
            return true;
        }
        if( user.equals("aihelpConsoleBack") && pwd.equals("mqttZmkm2018")) {
            return true;
        }
        if (AllowAnyUserPWD.size() > 0 ) {
            if (AllowAnyUserPWD.containsKey(user + ":" +pwd)) {
                return true;
            }
        }
        String newpwd = CommonUtils.stringMD5(user);
        if (!StringUtil.isNullOrEmpty(newpwd) && !newpwd.equals(pwd)) {
//            logger.info("Deny {} {} {} {}", user, pwd, newpwd, ip);
            return false;
        }
        return true;
    }
    public static String filename = "config.properties";
    public static void reloadIP() {
        Properties config = new Properties();
        try {
            InputStream input = null;
            input= new FileInputStream(filename);
            config.load(input);
            input.close();
        } catch (Exception ex ) {

        }
        String ips = config.getProperty("white_ip_list","").trim();
        String[] ip = ips.split("\\ ");
        AllowAnyTopicIp.clear();
        for ( String item: ip) {
            String normail_item = item.trim();
            if(StringUtil.isNullOrEmpty(normail_item)) {
                continue;
            }
            AllowAnyTopicIp.put(normail_item,Boolean.TRUE);
        }
        AllowAnyTopicIp.put("127.0.0.1",Boolean.TRUE);

        String userpwds = config.getProperty("white_user_list","").trim();
        String[] userpwd = userpwds.split("\\ ");
        AllowAnyUserPWD.clear();
        for ( String item: userpwd) {
            String normail_item = item.trim();
            if(StringUtil.isNullOrEmpty(normail_item)) {
                continue;
            }
            AllowAnyUserPWD.put(normail_item,Boolean.TRUE);
        }
    }

    public static void init() {
        reloadIP();
    }

    public static void main(String[] args) {
        reloadIP();

        for(Map.Entry<String,Boolean> item : AllowAnyUserPWD.entrySet()) {
            System.out.println("[" + item.getKey()+ "]");
        }

        for(Map.Entry<String,Boolean> item : AllowAnyTopicIp.entrySet()) {
            System.out.println( "[" + item.getKey() + "]" );
        }
    }

}
