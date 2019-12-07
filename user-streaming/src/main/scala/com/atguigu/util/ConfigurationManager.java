package com.atguigu.util;

import java.io.InputStream;
import java.util.Properties;

/**
 * 读取配置文件
 */
public class ConfigurationManager {

    private static Properties prop =new Properties();

    static {
        try {
            //反射加载
            InputStream input = ConfigurationManager.class.getClassLoader()
                    .getResourceAsStream("comerce.properties");
            prop.load(input);

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    //获取配置
    public static String getProperty(String key) {
        return prop.getProperty(key);

    }

    //获取布尔类型配置

    public boolean getBoolean(String key){
        String value = prop.getProperty(key);
        try {
            //解析是true还是false
            return Boolean.valueOf(value);
        }catch (Exception e){
            return false;
        }
    }
}
