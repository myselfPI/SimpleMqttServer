package com.longtech.mqtt.Utils;

import org.apache.commons.lang.StringUtils;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;

/**
 * Created by kaiguo on 2018/12/13.
 */
public class Constants {
    public static final String VERSION = "1.0.1";
    public static int CLIENT_TIMEOUT = 60;
    public static int SESSION_TASK_INTERVAL = 60;
    public static final int SESSION_INVALID_TIME_SEC = 300;
    public static final int USER_TASK_INTERVAL = 300;
    public static final int USER_INVALID_TIME_SEC = 7200;
    public static final int BYTES_DEBUG_THRESHOLD = 512;
    public static final String APP_DIR = "extensions/";
    public static final String APP_CONTEXT_FILE_NAME = "application-context.xml";
    public static String KEY_FILE = "";
    public static String CERT_FILE = "";

    public static final String HOST_NAME = "NETTY_MQTTSERVER_" + getSystemHostName() + "_" + (System.currentTimeMillis()/1000);

    public static String getPID() {
        String pidAtHost = ManagementFactory.getRuntimeMXBean().getName();
        String pid = StringUtils.split(pidAtHost, '@')[0];
        return pid;
    }

    public static String getSystemHostName() {
        String hostname = "default_name";
        try {
            hostname = InetAddress.getLocalHost().getHostAddress()+"_"+ CommonUtils.getIntValue("server_port",8020);
        } catch (Exception ex) {
        }
        return  hostname;
    }

    public static void init() {
        CLIENT_TIMEOUT = CommonUtils.getIntValue("client_timeout",60);
        KEY_FILE = CommonUtils.getValue("key_file", "aihelp.pkcs8_key.pem");
        CERT_FILE = CommonUtils.getValue("cert_file","aihelp.net.chained.crt");

    }
}
