package com.atguigu.util;

import com.alibaba.fastjson.JSONObject;

public class ParseJsonData {

    public static JSONObject getObject(String data) {
        try {
            return JSONObject.parseObject(data);
        } catch (Exception e) {
            return null;
        }
    }
}
