package com.wzc.Utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertityUtil {

    private static Properties properties = null;

    public static Properties getPropertity(){

        //加载静态资源
        InputStream inputStream = ClassLoader.getSystemResourceAsStream("kafka.properties");

        try {
            properties = new Properties();
            properties.load(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties;
    }
}
