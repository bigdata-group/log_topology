package topo;


import java.io.IOException;
import java.util.Properties;

import spouts.RedisSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.PvCounter;
import bolts.LogNormalizer;
import redis.clients.jedis.Jedis;

public class TopologyMain {
	
	public final static String REDIS_HOST = "localhost";
	public final static int REDIS_PORT = 6379;
	public final static int MYSQL_INTERVAL = 6000;
	public final static int STAT_DB = 3;
	
	
	public static boolean testing = true;
	
	public Jedis jedis;
	public String host; 
	public int port;

	public static void main(String[] args) throws InterruptedException, AlreadyAliveException, InvalidTopologyException {
        //
		Properties props = new Properties();
		try {
			props.load(TopologyMain.class.getResourceAsStream("/topo.properties"));
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

		Config.setNumAckers(conf, 0);
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




	
	}
}
