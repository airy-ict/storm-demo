package com.xiao.storm.demo.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;

import java.util.Properties;

/**
 * Created by xiaoliang
 * 2016/8/15 16:39
 *
 * @Version 1.0
 */
public class CalculateTopology {

    private static final Logger logger = LoggerFactory.getLogger(CalculateTopology.class);

    public void calculate(Properties properties){

        TopologyBuilder builder = new TopologyBuilder();
        //设置喷发节点并分配并发数，该并发数将会控制该对象在集群中的线程数（6个）
        String zkhost = (String)properties.get("kafka.zookeeper");
        String topic = (String)properties.get("kafka.topic");
        String groupId =(String)properties.get("kafka.groupId");
        ZkHosts zkHosts = new ZkHosts(zkhost);//kafaka所在的zookeeper
        SpoutConfig spoutConfig = new SpoutConfig(zkHosts, topic, "/storam-kafka/test",
                groupId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
        builder.setSpout("kafkaSpout", kafkaSpout, Integer.valueOf(properties.getProperty("kafkaConsumerSpout","6")));


//		builder.setSpout("kafkaConsumerSpout", new KafkaConsumerSpout(properties),1);
        //设置数据处理节点并分配并发数，指定该节点接收喷发节点的策略为随机模式(3个executor,6个task（一个task对应一个spout或者bolt）,所以：每个executor里有2个任务)
        //exector 是真正的并行度
        builder.setBolt("disGameLogBolt", new DisGameLogBolt(properties),
                Integer.valueOf(properties.getProperty("disGameLogBolt","6"))).localOrShuffleGrouping("kafkaSpout");
//        builder.setBolt("parseLogBolt", new ParseLogBolt(properties),
//                Integer.valueOf(properties.getProperty("parseLogBolt","12"))).localOrShuffleGrouping("disGameLogBolt");
//        builder.setBolt("removDupLogBolt", new RemovDupLogBolt(properties),
//                Integer.valueOf(properties.getProperty("removDupLogBolt","30"))).localOrShuffleGrouping("parseLogBolt");
//        builder.setBolt("dbLogBolt", new DbLogBolt(properties),
//                Integer.valueOf(properties.getProperty("dbLogBolt","30"))).localOrShuffleGrouping("removDupLogBolt");

        Config conf = new Config();
        // 调试信息
        conf.setDebug(false);
        // 和集群的slots有关,共有20个工作进程执行这个topology
        //是否需要提高workers：最好一台机器上的一个topogy只使用一个worker，这样减少了workers之间的数据传输
        //一个worker对应一个slot
        conf.setNumWorkers(Integer.valueOf(properties.getProperty("numWorkers","8")));
        //这个设置一个spout task上面最多有多少个没有处理的tuple（没有ack/failed）回复，推荐设置，以防止队列爆掉
        conf.setMaxSpoutPending(Integer.valueOf(properties.getProperty("maxSpoutPending","5000")));
//        conf.setMaxTaskParallelism(Integer.valueOf(properties.getProperty("maxTaskParallelism","100")));
        conf.setMessageTimeoutSecs(Integer.valueOf(properties.getProperty("messageTimeoutSecs","60")));

        // Topology 名称
        String topologyName = properties.getProperty("topology_name");
//		if(b){
//        try {
//            StormSubmitter.submitTopology(topologyName, conf,
//                    builder.createTopology());
//        } catch (AlreadyAliveException e) {
//            logger.error("moblie CalculateTopology init error : ",e);
//        } catch (InvalidTopologyException e) {
//            logger.error("moblie CalculateTopology init error : ",e);
//        }
//		}else{
			LocalCluster cluster = new LocalCluster();
			// Topology 名称
			cluster.submitTopology(topologyName, conf, builder.createTopology());
//		}
    }

}
