package com.longtech.mqtt.Utils;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Created by kaiguo on 2018/12/13.
 */
public class DiskUtilMap {
    private static ConcurrentHashMap<String, String> kv = new ConcurrentHashMap<>();
    private static ScheduledExecutorService mWorkingExecutor =  Executors.newSingleThreadScheduledExecutor();
    private static Object lockObj = new Object();
    public static void put(String key, String val)  {

        kv.put(key,val);

        mWorkingExecutor.execute(new Runnable() {
            @Override
            public void run() {
                long start = System.currentTimeMillis();
                synchronized (lockObj) {
                    try {
                        FileOutputStream fos = new FileOutputStream(new File("data/a.txt"));
                        ObjectOutputStream oos = new ObjectOutputStream(fos);
                        oos.writeObject(kv);
                        oos.flush();
                        oos.close();
                        fos.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                long end = System.currentTimeMillis();

                System.out.println("put spend " + (end - start));
            }
        });


    }

    public static String getVal(String key) {
        return kv.get(key);
    }

    public static String remove(String key) {
        String ret = kv.remove(key);
        mWorkingExecutor.execute(new Runnable() {

            @Override
            public void run() {
                long start = System.currentTimeMillis();
                synchronized (lockObj) {
                    try {
                        FileOutputStream fos = new FileOutputStream(new File("data/a.txt"));
                        ObjectOutputStream oos = new ObjectOutputStream(fos);
                        oos.writeObject(kv);
                        oos.flush();
                        oos.close();
                        fos.close();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

                long end = System.currentTimeMillis();

                System.out.println("remove spend " + (end - start));
            }
        });

        return key;
    }

    public static ConcurrentHashMap<String, String> getDiskMap() {
        ConcurrentHashMap<String, String> mapInFile = null;
        synchronized (lockObj) {
            try {
                FileInputStream fileIn = new FileInputStream("data/a.txt");
                ObjectInputStream in = new ObjectInputStream(fileIn);
                mapInFile = (ConcurrentHashMap<String,String>) in.readObject();
                in.close();
                fileIn.close();
            } catch (Exception e) {
            }
        }

        return mapInFile;
    }
}
