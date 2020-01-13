package com.longtech.mqtt.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by kaiguo on 2020/1/13.
 */
public abstract class WrapperRunnable implements Runnable {
    protected static Logger logger = LoggerFactory.getLogger(WrapperRunnable.class);
    @Override
    public void run() {
        try {
            execute();
        }catch (Exception e) {
            logger.error("RUNERROR", e);
        }
    }

    public abstract void execute();
}
