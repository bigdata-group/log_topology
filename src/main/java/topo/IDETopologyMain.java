package topo;


import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.Nimbus;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import bolts.LogNormalizer;
import bolts.PvCounter;
import org.json.simple.JSONValue;
import redis.clients.jedis.Jedis;
import spouts.RedisSpout;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class IDETopologyMain {
	
	public final static String REDIS_HOST = "localhost";
	public final static int REDIS_PORT = 6379;
	public final static int MYSQL_INTERVAL = 6000;
	public final static int STAT_DB = 3;
	
	
	public static boolean testing = true;
	
	public Jedis jedis;
	public String host; 
	public int port;

	public static void main(String[] args) throws Exception {
        //
		Properties props = new Properties();
		try {
			props.load(IDETopologyMain.class.getResourceAsStream("/topo.properties"));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		props.getProperty("a", "d");
		
		
        //Configuration
		Config conf = new Config();
		//conf.setMaxTaskParallelism(3);
		//conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 3);
		conf.setDebug(testing);
//		conf.setNumWorkers(10);
        conf.put("redis-host", REDIS_HOST);
        conf.put("redis-port", REDIS_PORT);
        conf.put("interval", MYSQL_INTERVAL);
        conf.put("stat-db", STAT_DB);
        //conf.put("webserver", WEBSERVER);
        //conf.put("download-time", DOWNLOAD_TIME);
        
		
        //Topology definition
		TopologyBuilder builder = new TopologyBuilder();
		
		//builder.setSpout("reader",new WordReader());
		
		//builder.setBolt("normalizer", new WordNormalizer())
		//	.shuffleGrouping("reader");
//		TailFileSpout Tailspout = new TailFileSpout("src/main/resources/test.log");
		RedisSpout rs = new RedisSpout();
		
		//builder.setSpout("generator",new WordGenerator(),1);
		builder.setSpout("generator",rs,1);
		
		builder.setBolt("normalizer", new LogNormalizer(),1)
			.shuffleGrouping("generator");
		
		builder.setBolt("counter", new PvCounter(),1)
			.fieldsGrouping("normalizer", new Fields("word"));
       
        //Topology run
//		LocalCluster cluster = new LocalCluster();
//		cluster.submitTopology("wordcount", conf, builder.createTopology());
//		Thread.sleep(60000000);
//		cluster.shutdown();
		StormSubmitter.submitTopology("kdt_pv_pageid", conf,builder.createTopology());


		conf.setNumWorkers(2);
		conf.setDebug(true);
		// ...
		// topology 其他配置信息等

		// 读取本地 Storm 配置文件
		// Utils.readStormConfig() 会读取 Storm 依赖 jar 包的默认配置文件，
		// 如 "\maven\repository\org\apache\storm\storm-core\0.9.3\storm-core-0.9.3.jar\defaults.yaml"，
		// 如果集群配置与默认配置有较大不同，还需要修改对应配置信息。或者直接构造map自己put
		Map stormConf = Utils.readStormConfig();
		stormConf.put("nimbus.host", "hd124");
		stormConf.putAll(conf);


		String inputJar = "E:\\workspace\\storm-demo\\target\\storm-demo-0.0.5-SNAPSHOT-shade.jar";
		NimbusClient nimbus = new NimbusClient(stormConf, "hd124", 6627);
		// Nimbus.Client client = NimbusClient.getConfiguredClient(stormConf).getClient(); stormConf中包含host和port就可以这样获取client

		// 使用 StormSubmitter 提交 jar 包
		String uploadedJarLocation = StormSubmitter.submitJar(stormConf, inputJar);
		String jsonConf = JSONValue.toJSONString(stormConf);
		nimbus.getClient().submitTopology("remotetopology", uploadedJarLocation, jsonConf, builder.createTopology());
	}
}
