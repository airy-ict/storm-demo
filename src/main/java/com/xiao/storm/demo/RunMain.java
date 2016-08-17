package com.xiao.storm.demo;

import com.xiao.storm.demo.storm.CalculateTopology;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by xiaoliang
 * 2016/8/15 16:32
 *
 * @Version 1.0
 */
public class RunMain {

    public static void main(String[] args) {
        CalculateTopology calculateTopology  = new CalculateTopology();
        Properties properties = new Properties();
        InputStream in = Object. class .getResourceAsStream( "/config.properties" );

        try {
            properties.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }

        calculateTopology.calculate(properties);

    }

}
