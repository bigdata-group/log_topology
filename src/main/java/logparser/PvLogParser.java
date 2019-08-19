package logparser;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PvLogParser {
	
//	private static final Pattern LINE_PATTERN = Pattern.compile(
//		  "(\\S+:)?(\\S+? \\S+?) \\S+? DEBUG \\S+? - DEMANDE_ID=(\\d+?) - listener (\\S+?) : (\\S+?)");
	
	private static final Pattern LINE_PATTERN = Pattern.compile(
			  ".+\"GET \\/\\d\\.gif\\?(\\S+) HTTP\\/\\d\\.\\d\" \\d+ \\d+ \"(.+)\" \"(.*)\" \"(.*)\" \\w+ \"(.*)\"");
	//regex = re.compile(r".+\"GET /\d\.gif\?([^\s]+) HTTP\/\d\.\d\" \d+ \d+ \"(.+)\" \"(.*)\" \"(.*)\" \w+ \"(.*)\"")
	private static final Pattern PHP_TIME = Pattern.compile("\\d{13}$");
	private static final Pattern SPM_PAT = Pattern.compile("([a-zA-Z]+)(\\d+).?(\\w*)");
	private static final Pattern KDT_PAT = Pattern.compile("^\\d{1,20}$");
	
	//private static final String DATE_PATTERN = null;

	public static Map<String,String> parse(String line) throws ParseException {
			
			String logTime = "none";
			String spm = "none";
			String fm = "none";
			String displayType = "none";
			String displayId = "0";
			String refererType = "none";
			String refererId = "none";
			String sourceType = "none";
			String sourceId = "none";
			String sourceTn = "none";
			String kdt_id = "0";
			String sessionId = "0";
			String isFans = "0";
			String fans_id = "none";
			
			Map<String,String> map=new HashMap<String,String>();
			
			Matcher m = LINE_PATTERN.matcher(line);
		    if (m.matches() && m.groupCount() >= 5) {

		    	String queryStr = m.group(1);
		    	String[] strArray = queryStr.split("&");
    	
		    	//queryDict initialize, parse queryStr into queryDict 
		    	Map<String,String> queryDict=new HashMap<String,String>();
		    	for (int i = 0; i < strArray.length; i++){
						String item = strArray[i];
						String[] itemArr = item.trim().split("=");
						if(itemArr.length == 2){
							queryDict.put(itemArr[0], itemArr[1]);
						}else{
							continue;
						}
				}

		    	
		    	
		    	
		    	//time

				if(queryDict.containsKey("time")){
					logTime = queryDict.get("time");
					if(PHP_TIME.matcher(logTime).matches()){
					 	//log_time = log_time[:-3];
						Long logTimeInt = Long.parseLong(logTime);
					 	logTime = new java.text.SimpleDateFormat("yyyy/MM/dd-HH:mm:ss").format(new java.util.Date(logTimeInt));
					}
				}
				
				
				
				
				//spm

				if(queryDict.containsKey("spm")){
					spm = queryDict.get("spm");
				}
				
				
				String[] spmArr = spm.split("_");
				String displaySpm = spmArr[spmArr.length-1].trim();
				Matcher dm = SPM_PAT.matcher(displaySpm);
				if(dm.matches()){
					displayType = dm.group(1);
					displayId = dm.group(2);
				}
				
				if(spmArr.length >= 2){
					
					String sourceSpm = spmArr[0].trim();
					Matcher sm = SPM_PAT.matcher(sourceSpm);
					if(sm.matches()){
						sourceType = sm.group(1);
						sourceId = sm.group(2);
						sourceTn = sm.group(3);
					}
					
					String refererSpm = spmArr[spmArr.length-2].trim();
					Matcher rm = SPM_PAT.matcher(refererSpm);
					if(rm.matches()){
						refererType = rm.group(1);
						refererId = rm.group(2);
					}

				}
		    	
			
				
				
				//fm
				
				if(queryDict.containsKey("fm")){
					fm = queryDict.get("fm");
				}
		    	
				
				

				//cookie info
				String cookie = m.group(5);
				String[] cookieArr = cookie.split(";");
				Map<String,String> cookieDict = new HashMap<String,String>();
		    	
				for (int i = 0; i < cookieArr.length; i++){
						String item = cookieArr[i];
						String[] itemArr = item.trim().split("=");
						if(itemArr.length == 2){
							cookieDict.put(itemArr[0], itemArr[1]);
						}else{
							continue;
						}
				}
						

				if(cookieDict.containsKey("_kdt_id_")){
					kdt_id = cookieDict.get("_kdt_id_");
				}
				if(!KDT_PAT.matcher(kdt_id).matches()){
					kdt_id = "0";
				}
				
				
				if(cookieDict.containsKey("KDTSESSIONID")){
					sessionId = cookieDict.get("KDTSESSIONID");
				}
				String verify_weixin_openid = "0";
				if(cookieDict.containsKey("verify_weixin_openid")){
					verify_weixin_openid = cookieDict.get("verify_weixin_openid");
				}
						

				if(cookieDict.containsKey("is_fans")){
					isFans = cookieDict.get("is_fans");
				}

				if(cookieDict.containsKey("fans_id")){
					isFans = cookieDict.get("fans_id");
				}
				
						
		    }

	    	map.put("logTime", logTime);		
	    	map.put("fm", fm);
	    	map.put("kdt_id", kdt_id);
	    	map.put("cookie",sessionId);
			map.put("displayType",displayType);
			map.put("displayId",displayId);
			map.put("sourceType",sourceType);
			map.put("sourceId",sourceId);
			map.put("refererType",refererType);
			map.put("refererId",refererId);

			
			return map;
	        
		}
	
	public static Map<String,String> parseInput(String line) throws ParseException{
		
		return null;
		
	
	}
}







