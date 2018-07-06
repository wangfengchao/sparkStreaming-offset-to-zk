package com.server;

import org.apache.log4j.Logger;

/**
 * Created by fc.w on 2018/05/31
 */
public class AppA {

    private static final Logger LOGGER = Logger.getLogger(AppA.class);
    public static void main(String[] args) throws InterruptedException {
        for (int i = 0; i < 20; i++) {
            LOGGER.info("Info [" + i + "]");
            Thread.sleep(1000);
        }
    }

}
