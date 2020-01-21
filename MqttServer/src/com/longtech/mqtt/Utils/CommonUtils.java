package com.longtech.mqtt.Utils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import io.netty.util.internal.StringUtil;

import java.io.*;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by kaiguo on 2018/8/30.
 */
public class CommonUtils {
    static Properties config = null;
    public static String filename = "config.properties";
    public static void loadConfig() {

//        String filename = "config.properties";

        System.out.println("use " + filename );
        try {
            InputStream input = null;
            input= new FileInputStream(filename);
            config.load(input);
            input.close();
        } catch (Exception ex ) {

        }
    }

    public static String getValue( String key, String defaultValue ) {
        if( config == null) {
            config = new Properties();
            loadConfig();
        }
        String wbs = config.getProperty(key);
        if(StringUtil.isNullOrEmpty(wbs)) {
            return defaultValue;
        }
        else {
            return wbs;
        }
    }

    public static int getIntValue( String key, int defaultValue) {
        String ret = getValue(key,"");
        if( StringUtil.isNullOrEmpty(ret)) {
            return defaultValue;
        }

        try {
            int val = Integer.parseInt(ret);
            return val;
        } catch (Exception e){

        }
        return defaultValue;
    }


    public static Map<String, String> MakePairs(String input)
    {
        Map<String, String> retVal = new HashMap<>();
        if( StringUtil.isNullOrEmpty(input)) {
            return retVal;
        }

        Map<String, String> query_pairs = new LinkedHashMap<String, String>();
        String query = input;
        String[] pairs = query.split("&");
        for (String pair : pairs) {
            int idx = pair.indexOf("=");
            try {
                query_pairs.put(URLDecoder.decode(pair.substring(0, idx), "UTF-8"), URLDecoder.decode(pair.substring(idx + 1), "UTF-8"));

            } catch (Exception e) {

            }
        }
        return query_pairs;
    }

    public static String stringMD5(String info) {
        try {
            //获取 MessageDigest 对象，参数为 MD5 字符串，表示这是一个 MD5 算法（其他还有 SHA1 算法等）：
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            //update(byte[])方法，输入原数据
            //类似StringBuilder对象的append()方法，追加模式，属于一个累计更改的过程
            md5.update(info.getBytes("UTF-8"));
            //digest()被调用后,MessageDigest对象就被重置，即不能连续再次调用该方法计算原数据的MD5值。可以手动调用reset()方法重置输入源。
            //digest()返回值16位长度的哈希值，由byte[]承接
            byte[] md5Array = md5.digest();
            //byte[]通常我们会转化为十六进制的32位长度的字符串来使用,本文会介绍三种常用的转换方法
            return byteArrayToHex(md5Array);
        } catch (NoSuchAlgorithmException e) {
            return "";
        } catch (UnsupportedEncodingException e) {
            return "";
        }
    }

    public static String byteArrayToHex(byte[] byteArray)
    {
        // 首先初始化一个字符数组，用来存放每个16进制字符
        char[] hexDigits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F' };
        // new一个字符数组，这个就是用来组成结果字符串的（解释一下：一个byte是八位二进制，也就是2位十六进制字符（2的8次方等于16的2次方））
        char[] resultCharArray = new char[byteArray.length * 2];
        // 遍历字节数组，通过位运算（位运算效率高），转换成字符放到字符数组中去
        int index = 0;
        for (byte b : byteArray)
        {
            resultCharArray[index++] = hexDigits[b >>> 4 & 0xf];
            resultCharArray[index++] = hexDigits[b & 0xf];
        }
        return new String(resultCharArray).toLowerCase();
    }
    static SimpleDateFormat dateformateYmd = new SimpleDateFormat("yyyyMMdd");
    static SimpleDateFormat dateformateYmdHHii = new SimpleDateFormat("yyyyMMddHHmm");
    static SimpleDateFormat dateformateYmdHH = new SimpleDateFormat("yyyyMMddHH");
    public static  String getYmd() {
        return dateformateYmd.format(new Date());
    }
    public static  String getYmdHi() { return dateformateYmdHHii.format(new Date());}
    public static  String getYmdH() { return dateformateYmdHH.format(new Date());}

    public static boolean hasNullOrEmpty(String ... args) {
        for( String arg : args) {
            if( StringUtil.isNullOrEmpty(arg)) {
                return true;
            }
        }
        return false;
    }
    public static boolean veryfy(Map<String,String> params) {
        return veryfy(params,"kd8743udakd",Arrays.asList(new String[]{"sign"}));
    }

    public static boolean veryfy(Map<String,String> params, String secret) {
        return veryfy(params,secret,Arrays.asList(new String[]{"sign"}));
    }

    public static boolean veryfy(Map<String,String> params, String secret, Collection<String> except) {
        if( StringUtil.isNullOrEmpty(params.get("sign"))) {
            return false;
        }
        String[] keys = new String[params.size()];
        keys= params.keySet().toArray(keys);
        Arrays.sort(keys);
        StringBuffer sb = new StringBuffer();
        for( String key : keys ) {
            if( except.contains(key)) {
                continue;
            }

            sb.append(key).append(params.get(key));
        }
        sb.append(secret);

        String base = sb.toString();
        String signednew = stringMD5(base);

        String singedOld = params.get("sign");
        if( singedOld.equals(signednew )) {
            return true;
        }

        return false;
    }

    public static String toJSONString(Object obj ) {
        return JSON.toJSONString(obj, SerializerFeature.BrowserCompatible);
    }

    public static <T> T getBean(ConcurrentHashMap map, String name, Class<T> cls) {
        T b1 = (T) map.get(name);
        if (b1 != null) {
            return b1;
        }
        try {
            b1 = cls.newInstance();
            T b2 = (T) map.putIfAbsent(name, b1);
            if (b2 != null) {
                return b2;
            }
            return b1;
        } catch (Exception e ) {

        }
        return null;
    }

//    public static void writeThreadID() {
//        try {
//            FileOutputStream fos = new FileOutputStream(new File("sid.txt"));
//            ObjectOutputStream oos = new ObjectOutputStream(fos);
//            oos.writeObject(new Long(1));
//            oos.flush();
//            oos.close();
//            fos.close();
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }

    public static long getConfig() {
        try {
            FileInputStream fileIn = new FileInputStream("sid.txt");
            BufferedReader bis = new BufferedReader(new InputStreamReader(fileIn));
            String text = bis.readLine();
            bis.close();
            return Long.parseLong(text);
        } catch (Exception e) {
        }
        return 0L;
    }

    public static long getRandomNum( long size ) {
        if( size == 1) {
            return 0;
        }
        ThreadLocalRandom generator = ThreadLocalRandom.current();
        long randnum = generator.nextLong(size);
        return randnum;
    }
}
