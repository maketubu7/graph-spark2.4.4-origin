package com.sugon.nebula.jgraph.tools;

import com.alibaba.fastjson.JSON;
import java.io.InputStream;
import java.util.*;

public class propertiesTool {

    public static void main(String[] args) {
        String user = getPropertyValue("druid.properties", "username");
        System.out.println(user);
    }

    public static String getPropertyValue(String filename, String name) {
        Properties paramProp = new Properties();
        InputStream inputStream = propertiesTool.class.getResourceAsStream("/" + filename);
        String values = null;
        try {
            paramProp.load(inputStream);
            values = paramProp.getProperty(name);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                inputStream.close();
            } catch (Exception e) {
            }
        }
        return values;
    }


    public static Properties getKafkaProperties(String fileName) {
        Properties paramProp = new Properties();
        Properties kafkaProp = new Properties();
        InputStream inputStream = propertiesTool.class.getResourceAsStream("/" + fileName);
        Map<String, Object> map = new HashMap<String, Object>();
        try {
            paramProp.load(inputStream);
            Set<Map.Entry<Object, Object>> set = paramProp.entrySet();
            Iterator itr = set.iterator();
            while (itr.hasNext()) {
                Map.Entry<Object, Object> entry = (Map.Entry<Object, Object>) itr.next();
                kafkaProp.setProperty((String) entry.getKey(), (String) entry.getValue());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                inputStream.close();
            } catch (Exception e) {
            }
        }
        return kafkaProp;
    }


    public static <T> String Object2JsonStr(T tk) {
        return JSON.toJSONString(tk);
    }
}