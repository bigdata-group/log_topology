
package utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import backtype.storm.Config;
import redis.clients.jedis.Jedis;

public class DataSyncer implements Runnable {

	private static final Pattern PAGE_ID = Pattern.compile("^\\d{1,10}$");
	
    private final long delay;
    
    private final boolean end;

    private volatile boolean run = true;

    private Map conf;
    
    public DataSyncer(Map conf) {
        this(conf,1000);
    }

    public DataSyncer(Map conf, long delay) {
        this(conf, 1000, false);
    }

    public DataSyncer(Map conf, long delay, boolean end) {

        this.conf = conf;
        this.delay = delay;
        this.end = end;

    }

    public static DataSyncer create(Map conf, long delay, boolean end) {
    	DataSyncer DataSyncer = new DataSyncer(conf, delay,end);
        Thread thread = new Thread(DataSyncer);
        thread.setDaemon(true);
        thread.start();
        return DataSyncer;
    }


    public static DataSyncer create(Map conf, long delay) {
        return create(conf, delay, false);
    }

    public static DataSyncer create(Config conf) {
        return create(conf, 1000, false);
    }

    public Map getConf() {
        return conf;
    }

    public long getDelay() {
        return delay;
    }

    public void run() {
			String host = conf.get("redis-host").toString();
			int port = Integer.valueOf(conf.get("redis-port").toString());
			Jedis jedis = new Jedis(host, port);
			jedis.select(3);
			
			// mysql connection
			ConnectDB cd=new ConnectDB();
			Connection conn = cd.ConnectMysql();
			
			try{
				Statement stmt = conn.createStatement();
		    }catch(Exception e){
		    	e.printStackTrace();
		    }
			
			Integer testId = 1;
			while(run){
				//test
				//
				System.out.println("fuck mysql");
//				testId += 1;
//				String testSql="REPLACE INTO goods_pv(id,pv,uv,store_pv,store_uv) VALUES(?,2,3,4,5);";
//				try {
//					PreparedStatement testps = conn.prepareStatement(testSql);
//					testps.setLong(1,testId);
//					testps.execute();
////					int result=ps.execute();//line num or 0
//////							if(result>0)
//////								return true;
//				} catch (SQLException ex) {
//					ex.printStackTrace();
//				}
				
				
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				// keys * & values
			    Set keys = jedis.keys("*");
			    
			    //keys 为空
			    
			      
			    
			    //test
//			    Integer keysLen = keys.size() ;
//			    System.out.println("keys len : " + keysLen.toString());
			    
				Iterator t1=keys.iterator() ;
//				String pageType = "n";  
//				Long pageId = (long) 0; 
//				String numType = "n"; 
//				Integer pv = 0; 
//				Integer uv = 0; 
//				Integer store_pv = 0; 
//				Integer store_uv = 0; 
				
				while(t1.hasNext()){
					
					
					String pageType = "n";  
					Long pageId = (long) 0; 
					//String numType = "n"; 
					Integer pv = 0; 
					Integer uv = 0; 
					Integer store_pv = 0; 
					Integer store_uv = 0; 
					
					
					Object obj1 = t1.next();
					String sKey = obj1.toString(); 
					
					//test
				    //System.out.println("a key inredis: " + sKey);
					
					//String keyType = jedis.type(sKey);
					String keyType = jedis.type(sKey);
					System.out.println(sKey + "'s type is " + keyType);
					//values 
					if(keyType.equals("hash")){
						String[] keyArr = sKey.split("_");
						//Integer sValue = Integer.parseInt(jedis.get(sKey));
						if(keyArr.length == 2){
							pageType = keyArr[0];
							//pageId = Long.parseLong(keyArr[1]); 
//							if(keyArr[1].length() > 10){
//								continue;
//							}
							
							if(!PAGE_ID.matcher(keyArr[1]).matches()){
								continue;
							}
							pageId = Long.parseLong(keyArr[1]); 
							
							pv = Integer.parseInt(jedis.hget(sKey,"pv"));
							uv = Integer.parseInt(jedis.hget(sKey,"uv"));
							store_pv = Integer.parseInt(jedis.hget(sKey,"store_pv"));
							store_uv = Integer.parseInt(jedis.hget(sKey,"store_uv"));
							
							
							//numType = keyArr[2];
							/*
							Map<String,Integer> valueMap = new HashMap<String, Integer>();
							valueMap.put("pv", 1);
							valueMap.put("uv", 2);
							valueMap.put("store_pv", 3);
							valueMap.put("store_uv", 4);
										 
							switch(valueMap.get(numType))
							{    
								   case 1:
								       pv = sValue;
								       break;
								   case 2:
								       uv = sValue;
								       break;
								   case 3:
								       store_pv = sValue;
								       break;
								   case 4:
								       store_uv = sValue;
								       break;
								   default:
									   break;
							}
						
							
						}
						*/
							
						//
						String tableName ="";
//						Map<String,Integer> pageMap=new HashMap<String, Integer>();
//						pageMap.put("g", 1);
//						pageMap.put("f", 2);
//						
//									 
//						switch(pageMap.get(pageType))
//						{    
//							   case 1:
//							       tableName = "goods";
//							       break;
//							   case 2:
//							       tableName = "feature";
//							       break;
//							   default:
//								   break;
//						}
						if(pageType.equals("g")){
							tableName = "goods";
						}else if(pageType.equals("f")){
							tableName = "features";
						}else{
							tableName = "none";
						}
						
						if(tableName.equals("goods") == true){
							tableName = tableName + "_pv";
						}else if(tableName.equals("features") == true){
							tableName = tableName + "_pv";
						}else{
							continue;
						}
						
						try {
							String inSql="REPLACE INTO "+ tableName +" (id,pv,uv,store_pv,store_uv) VALUES(?,?,?,?,?); ";
							PreparedStatement ps = conn.prepareStatement(inSql);
							ps.setLong(1,pageId);
							ps.setLong(2,pv);
							ps.setLong(3,uv);
							ps.setLong(4,store_pv);
							ps.setLong(5,store_uv);
							
				
							
							
//							String inSql="REPLACE INTO goods_pv (id,pv,uv,store_pv,store_uv) VALUES(?,?,?,?,?);";
//							PreparedStatement ps = conn.prepareStatement(inSql);
//							ps.setLong(1,pageId);
//							ps.setLong(2,pv);
//							ps.setLong(3,uv);
//							ps.setLong(4,store_pv);
//							ps.setLong(5,store_uv);
							
							ps.execute();
//							int result=ps.executeUpd ate();//line num or 0
//							if(result>0)
//								return true;
						} catch (SQLException ex) {
							ex.printStackTrace();
						}
					}

				}
				
				
				
				}

			}
    }

    public void stop() {
        this.run = false;
    }
}



//