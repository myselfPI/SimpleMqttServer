package com.longtech.mqtt.Utils;

import org.apache.commons.lang.StringUtils;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by kaiguo on 2018/9/14.
 */
public class NetUtil {
    public static String postString( String url, String content ) {
        String val = HttpClient.doPost(url,content);
        return val;
    }
}

