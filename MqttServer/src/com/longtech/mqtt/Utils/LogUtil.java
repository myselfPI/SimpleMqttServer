package com.longtech.mqtt.Utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


import java.util.Map;

import io.netty.util.internal.StringUtil;
import org.slf4j.Logger;

/**
 * Created by kaiguo on 2018/10/19.
 */
public class LogUtil {
    public void outputLog( String uid, String method, String sessionid, String data, int level ) {
        JSONObject obj = new JSONObject();
        obj.put("uid",uid);
        obj.put("method", method);
        obj.put("sessionid", sessionid);
        obj.put("data", data);
    }

//    public static void writelog( Logger logger, String appid, String uid, String cmd, Object params,  int event ) {
//        JSONObject jsonObj = new JSONObject();
//        jsonObj.put("params",params);
//        logger.info("LU {} {} _{}_ work{} {} {} {}", appid, uid, "0", "0", cmd, event, CommonUtils.toJSONString(jsonObj));
//    }
//
//    public static void writelog( Logger logger, AbstractSession session, String data, Object params, int event ) {
//
//        String appid = (String)session.getProperty("appid");
//        String uid = (String)session.getProperty("uid");
//        int sid = session.getSessionId();
//        String method = "";
//        int workerid = session.getWorkIndex();
//        if( data.startsWith("login") || data.startsWith("logout") || data.equals("closed") || data.equals("another.login")) {
//            method = data;
//            data = CommonUtils.toJSONString(params != null ? params: new JSONObject());
//        }
//        else {
//            method = "command";
////            data = JSON.toJSONString(new JSONObject());
//            if( event == 0 ) {
//                data = data.replace('\n', ' ');
//                data = data.replace('\r', ' ');
//            }
//        }
//        if(StringUtil.isNullOrEmpty(appid)) {
//            if( params != null ) {
//                Map mapParams = (Map)params;
//                appid = (String)mapParams.get("appid");
//            }
//
//        }
//        if(StringUtil.isNullOrEmpty(uid)) {
//            if( params != null ) {
//                Map mapParams = (Map) params;
//                uid = (String) mapParams.get("uid");
//            }
//        }
//
//        logger.info("LU {} {} _{}_ work{} {} {} {}", appid, uid, sid, workerid, method, event, data);
//    }
//
//    public static void writeErrorLog( Logger logger, AbstractSession session, String data, Object params, int event, Exception ex ) {
//        String appid = (String)session.getProperty("appid");
//        String uid = (String)session.getProperty("uid");
//        int sid = session.getSessionId();
//        String method = "";
//        int workerid = session.getWorkIndex();
//        if( data.startsWith("login") || data.startsWith("logout") || data.equals("closed") || data.equals("another.login")) {
//            method = data;
//            data = CommonUtils.toJSONString(params != null ? params: new JSONObject());
//        }
//        else {
//            method = "command";
////            data = JSON.toJSONString(new JSONObject());
//            if( event == 0 ) {
//                data = data.replace('\n', ' ');
//            }
//        }
//        if(StringUtil.isNullOrEmpty(appid)) {
//            if( params != null ) {
//                Map mapParams = (Map)params;
//                appid = (String)mapParams.get("appid");
//            }
//
//        }
//        if(StringUtil.isNullOrEmpty(uid)) {
//            if( params != null ) {
//                Map mapParams = (Map) params;
//                uid = (String) mapParams.get("uid");
//            }
//        }
//
//        logger.error("LU {} {} _{}_ work{} {} {} {}", appid, uid, sid, workerid, method, event, data, ex);
//
//    }
}
