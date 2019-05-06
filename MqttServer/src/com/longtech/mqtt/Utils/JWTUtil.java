package com.longtech.mqtt.Utils;


import com.google.api.client.util.Base64;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.Charset;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.StringTokenizer;

/**
 * Created by kaiguo on 2018/12/13.
 */
public class JWTUtil {
    public static boolean checkPassword(String user, String password) {
        try {


            if( user.equals("hello") || user.equals("cok_server") || user.equals("cluster")) {
                StringTokenizer st = new StringTokenizer(password,".");
                String base64Header = st.nextToken();
                String base64Claim = st.nextToken();
                String oldsignature = st.nextToken();

                String Claim = new String(Base64.decodeBase64(base64Claim), Charset.forName("UTF8"));
                if( Claim.startsWith("{\"name\":\"bob\", \"age\":50")) {
                    String newsignature = Hmacsha256(secret, base64Header + "." + base64Claim);
                    if( oldsignature.equals(newsignature)) {
                        return true;
                    }
                }
            }
        } catch (Exception e) {

        }

        return true;
    }
    private static final  String secret = "1234567890";
    private static final  String MAC_INSTANCE_NAME = "HMacSHA256";
    public static String Hmacsha256(String secret, String message) throws NoSuchAlgorithmException, InvalidKeyException {
        Mac hmac_sha256 = Mac.getInstance(MAC_INSTANCE_NAME);
        SecretKeySpec key = new SecretKeySpec(secret.getBytes(), MAC_INSTANCE_NAME);
        hmac_sha256.init(key);
        byte[] buff = hmac_sha256.doFinal(message.getBytes());
        return Base64.encodeBase64URLSafeString(buff);
    }
}
