package spouts;

import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

import logparser.PvLogParser;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;

import redis.clients.jedis.Jedis;
import utils.MyString;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;



public class RedisSpout extends BaseRichSpout{

	private static final long serialVersionUID = 4071287265800284501L;
	Jedis jedis;
	String host; 
	int port;
	SpoutOutputCollector collector;
			
	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map stormConf, TopologyContext context,
			SpoutOutputCollector collector) {
		host = stormConf.get("redis-host").toString();
		port = Integer.valueOf(stormConf.get("redis-port").toString());
		this.collector = collector;
		reconnect();
	}
	
	private void reconnect() {
		jedis = new Jedis(host, port);
		jedis.select(2);
	}

	@Override
	public void nextTuple() {
		//String content = jedis.rpop("navigation");
		String content = jedis.rpop("raw");
		if(content==null || "nil".equals(content)) {
			try { Thread.sleep(10); } catch (InterruptedException e) {}
		} else {
	        JSONObject obj=(JSONObject)JSONValue.parse(content);
	        String message = obj.get("message").toString();
//	        String product = obj.get("product").toString();
//	        String type = obj.get("type").toString();
//	        HashMap<String, String> map = new HashMap<String, String>();
//	        map.put("product", product);
//	        NavigationEntry entry = new NavigationEntry(user, type, map);
	        String line = "";
	        try {
				Map<String,String> map = PvLogParser.parse(message);
				
				/*
		    	map.put("logTime", logTime);		
		    	map.put("fm", fm);
		    	map.put("kdt_id", kdt_id);
		    	map.put("cookie",sessionId);
				map.put("displayType",displayType);
				map.put("displayId",displayId);
				map.put("sourceType",sourceType);
				map.put("sourceId",sourceId);
				*/
				
				String[] strArr = {map.get("logTime"),
						map.get("fm"),map.get("kdt_id"),
						map.get("cookie"),map.get("displayType"),
						map.get("displayId"),map.get("sourceType"),
						map.get("sourceId"),map.get("refererType"),
						map.get("refererId")};
				MyString ms = new MyString();
				line = ms.combine(strArr, "\t");
			} catch (ParseException e) {
				e.printStackTrace();
			}
	        if(line != ""){
	        	collector.emit(new Values(line));
	        }
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("line"));
	}
	
	
}
