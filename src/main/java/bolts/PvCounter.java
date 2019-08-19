package bolts;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import redis.clients.jedis.Jedis;
import utils.DataSyncer;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;

public class PvCounter extends BaseBasicBolt {

	private DataSyncer syncer;
	private long interval;
	private Integer id;
	private String name;
	private Map<String, Integer> counters;
	private BufferedWriter out = null;
	private Integer pv = 0;
	private Jedis jedis;
	private String host; 
	private int port;
	private int db;


	@Override
	public void cleanup() {
		/*
		try {
            //BufferedWriter out = new BufferedWriter(new FileWriter("src/main/resources/out.txt"));
            System.out.println("-- Word Counter ["+name+"-"+id+"] --");
            for (Map.Entry<String, Integer> entry : counters.entrySet()) {
                System.out.println(entry.getKey()+": "+entry.getValue());            
                out.write(entry.getKey()+": "+entry.getValue()+"\n");                
            }
            out.close();
        } catch (IOException e) {
        	e.printStackTrace();
        }
        */
		try {
			out.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		syncer.stop();
		
	}

	//connect redis
	private void reconnect() {
		jedis = new Jedis(host, port);
		jedis.select(db);
		//System.out.println("redis connected");
		//jedis.set("redis connected","1");
	}
	
	/**
	 * On create
	 */
	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.counters = new HashMap<String, Integer>();
		this.name = context.getThisComponentId();
		this.id = context.getThisTaskId();
		this.pv = 0;
		
		//out to redis
		host = stormConf.get("redis-host").toString();
		port = Integer.valueOf(stormConf.get("redis-port").toString());
		db = Integer.valueOf(stormConf.get("stat-db").toString());
		reconnect();

        //out to file
//		try {
//			this.out = new BufferedWriter(new FileWriter("/Users/work/work/log_topology/out.txt"),1);
//		} catch (IOException e1) {
//			// TODO Auto-generated catch block
//			System.out.println("Error open file out.txt !"); 
//			e1.printStackTrace();
//		}
		
		
		// redis to mysql
		interval = Long.valueOf(stormConf.get("interval").toString());
		syncer = DataSyncer.create(stormConf,interval);
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}


	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
		String line = str.trim();
		String[] lineArr = (String[]) line.split("\t");
		Map <String,String> map = new HashMap<String,String>();
		Boolean flag = true;
		if(lineArr.length == 10){
	    	map.put("logTime", lineArr[0]);		
	    	map.put("fm", lineArr[1]);
	    	map.put("kdt_id", lineArr[2]);
	    	map.put("cookie",lineArr[3]);
			map.put("displayType",lineArr[4]);
			map.put("displayId", lineArr[5]);
			map.put("sourceType", lineArr[6]);
			map.put("sourceId", lineArr[7]);
			map.put("refererType", lineArr[8]);
			map.put("refererId", lineArr[9]);
		}else{
			flag = false;
		}
		
		
		
		//Compute 
		//if(flag && map.get("fm") == "display" && map.get("displayType") == "g"){
		if(flag == true && map.get("fm").equals("display") == true 
				&& map.get("displayType").equals("SI") == false
				&& map.get("displayType").equals("none") == false){
			
			//total pv
			if(!jedis.exists("total_pv")){
				jedis.hset("total_pv","pv","0");
				jedis.hset("total_pv","uv","0");
			}
			Integer totalPv = Integer.parseInt(jedis.hget("total_pv","pv"));
			totalPv += 1;
			jedis.hset("total_pv","pv",totalPv.toString());
			
			//total uv 
			String cookie = map.get("cookie");
			if(!jedis.exists("all_users")){
				jedis.sadd("all_users", cookie); 
			}
			jedis.sadd("all_users", cookie);
			Long totalUv = jedis.scard("all_users");			
//			if(!jedis.exists(uvKey)){
//				jedis.set(uvKey,"1");
//			}
			jedis.hset("total_pv","uv",totalUv.toString());
			
			
			
			//page id pv
			String pvKey = map.get("displayType") + "_" + map.get("displayId");
			if(!jedis.exists(pvKey)){
				jedis.hset(pvKey,"pv","0");
				jedis.hset(pvKey,"uv","0");
				jedis.hset(pvKey,"store_pv","0");
				jedis.hset(pvKey,"store_uv","0");
			}
			Integer pv = Integer.parseInt(jedis.hget(pvKey,"pv"));
			pv += 1;
			jedis.hset(pvKey,"pv",pv.toString());
			
			//page id uv
			String usersKey = map.get("displayType") + "_" + map.get("displayId") + "_users";
			//String cookie = map.get("cookie");
			if(!jedis.exists(usersKey)){
				jedis.sadd(usersKey, cookie); 
			}
			jedis.sadd(usersKey, cookie);
			Long uv = jedis.scard(usersKey);			
			String uvKey = map.get("displayType") + "_" + map.get("displayId");
//			if(!jedis.exists(uvKey)){
//				jedis.set(uvKey,"1");
//			}
			jedis.hset(uvKey,"uv",uv.toString());
			
			
			
			if(map.get("displayType").equals("g") == true && 
					map.get("refererType").equals("none") == false){
				
				//page id store_pv
				String storePvKey = map.get("refererType") + "_" + map.get("refererId");
				if(!jedis.exists(storePvKey)){
					jedis.hset(storePvKey,"pv","0");
					jedis.hset(storePvKey,"uv","0");
					jedis.hset(storePvKey,"store_pv","0");
					jedis.hset(storePvKey,"store_uv","0");
				}
				Integer storePv = Integer.parseInt(jedis.hget(storePvKey,"store_pv"));
				storePv += 1;
				jedis.hset(storePvKey,"store_pv",storePv.toString());
				
				//page id store_uv
				String storeUsersKey = map.get("refererType") + "_" + map.get("refererId") + "_storeUsers";
				//String cookie = map.get("cookie");
				if(!jedis.exists(storeUsersKey)){
					jedis.sadd(storeUsersKey, cookie); 
				}
				jedis.sadd(storeUsersKey, cookie);
				Long storeUv = jedis.scard(storeUsersKey);			
				String storeUvKey = map.get("refererType") + "_" + map.get("refererId");
				jedis.hset(storeUvKey,"store_uv",storeUv.toString());

			}
		}
	
//		if(flag == true){
//			try {
//				out.write(map.get("displayId") + "\n");
//				out.flush();
//	        } catch (IOException e) {
//	        	e.printStackTrace();
//	        }
//
//		}
	}
	
}


