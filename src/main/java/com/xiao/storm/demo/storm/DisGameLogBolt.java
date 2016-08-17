package com.xiao.storm.demo.storm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * Created by xiaoliang
 * 2016/8/16 10:41
 *
 * @Version 1.0
 */
public class DisGameLogBolt implements IBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(DisGameLogBolt.class);

    public DisGameLogBolt(Properties properties) {
        logger.debug("init class DisGameLogBolt ");

    }


    public void prepare(Map map, TopologyContext topologyContext) {
        logger.debug("init class DisGameLogBolt prepare");

    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        logger.debug("init class DisGameLogBolt execute");
        String log = tuple.getString(0);
        System.out.println(log);
    }

    public void cleanup() {
        logger.debug("init class DisGameLogBolt cleanup");

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        logger.debug("init class DisGameLogBolt declareOutputFields");

    }

    public Map<String, Object> getComponentConfiguration() {
        logger.debug("init class DisGameLogBolt getComponentConfiguration");

        return null;
    }
}
