package com.longtech.mqtt.Utils;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by kaiguo on 2018/8/30.
 */
public class CMDUtil {

    public static Map makeCMD(String cmd, Object data)
    {
        Map ret = new HashMap();
        ret.put("cmd", cmd);
        ret.put("data",data);
        ret.put("serverTime", System.currentTimeMillis());
        return ret;
    }

    public static Map makeCMD(String cmd, String key, Object data)
    {
        Map ret = new HashMap();
        ret.put("cmd", cmd);
        ret.put(key,data);
        ret.put("serverTime", System.currentTimeMillis());
        return ret;
    }

    public static boolean filterCMD(String appId, String uid, String cmd)
    {
//        if (strpos($uid, 'supervisor') === 0) {
//            if (!in_array($cmd, array("room.join"))) {
//                return false;
//            }
//        }
//        return true;
        return false;
    }
}
