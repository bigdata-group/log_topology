#!/usr/local/bin/python
#coding:utf-8

import os,sys,re,time
from urllib import unquote_plus
from urlparse import urlparse,parse_qs
from geoip import *

regex = re.compile(r".+\"GET /\d\.gif\?([^\s]+) HTTP\/\d\.\d\" \d+ \d+ \"(.+)\" \"(.*)\" \"(.*)\" \w+ \"(.*)\"")
client_regex = re.compile(r"MicroMessenger[\/\\]([\d\.]{3})")
source_regex = re.compile(r"([a-zA-Z]+)(\d+)\.?(\w*)")
share_regex = re.compile(r"(^http\:\/\/shop[\d]+\.koudaitong\.com)")
url_regex = re.compile(r"^https?\:\/\/([^\/]+\/)+[^\/\?]*\?(.*)")
android_regex = re.compile(r".+\s+\(Linux;\s+(?:U;\s+)?Android\s+([\d\.]+);\s+(?:[\w-]+;\s+)?(.+)\s+(?:Build\/.+)?\)\s+AppleWebKit.*")
iphone_regex = re.compile(r".+\s+\(iPhone;\s+CPU\s+iPhone\s+OS\s+([\d_]+) like.*")
kdt_regex = re.compile("^\d{1,20}$")
php_time = re.compile("\d{13}$")
#region_regex = re.compile(r"([^\xe7\x9c\x81]*)\xe7\x9c\x81([^\xe5\xb8\x82]*)(\xe5\xb8\x82)?")
#region_regex = re.compile(r"^([^\xe7\x9c\x81]*)\xe7\x9c\x81")
ip_locater = IpLocater( "QQWry.Dat".lower())


if __name__ == "__main__":
	
	pfile = open("province.txt","r")
	plist = set()
	for line in pfile:
		line = line.strip()
		plist.add(line)
	pfile.close()

	for line in sys.stdin:

		line = line.strip()
		m = regex.match(line)

		if not m:
			continue
		if len(m.groups()) < 5:
			continue

		query_str = m.group(1)
		display_url = m.group(2)
		
		is_share = 0
		if share_regex.search(display_url):
			is_share = 1

		n = url_regex.match(display_url)
		display_params = {}
		if n and len(n.groups()) > 1:
			display_query = m.group(2)
			arr = display_query.split("&")
			for item in arr:
				if not "=" in item:
					continue
				try:
					key,value = item.strip().split("=")
				except:
					continue
				display_params[key] = value
		
		scan = "none"
		if "scan" in display_params:
			scan = display_params["scan"]
		activity = "none"
		if "activity" in display_params:
			activity = display_params["activity"]

		'''
		# using urlparse 
		display_res = urlparse(display_url)  
		display_params = parse_qs(display_res.query)
		#qr code
		scan = "none"
		if "scan" in display_params:
			scan = display_params["scan"][0]
		activity = "none"
		if "activity" in display_params:
			activity = display_params["activity"][0]
		'''

		ua = m.group(3)
		ip = m.group(4)
		cookie = m.group(5)

		arr = query_str.split("&")
		query_dict = {}
		for item in arr:
			if not "=" in item:
				continue
			try:
				key,value = item.strip().split("=")
			except:
				continue
			query_dict[key] = value
		
		log_time = "none"
		if "time" in query_dict:
			log_time = query_dict["time"]
			if php_time.match(log_time):
			 	#log_time = log_time[:-3]
				log_time_int = int(log_time)/1000
			 	log_time = time.strftime("%Y/%m/%d-%H:%M:%S",time.localtime(log_time_int))
				log_time = log_time+'-'+str(log_time_int)
			else:
				log_time = "none"
		
		spm = "none"
		if "spm" in query_dict:
			spm = query_dict["spm"]
	
		#display
		display_type = "none"
		display_id = "none"
		referer_type = "none"
		referer_id = "none"
		source_type = "none"
		source_id = "none"
		source_tn = "none"
		
		spm_path_arr = spm.split("_")	
		display_spm = spm_path_arr[-1].strip()
		dm = source_regex.match(display_spm)
		if dm:
			display_type = dm.group(1)
			display_id = dm.group(2)

		#print display_type
		
		if len(spm_path_arr) >= 2:
			source_spm = spm_path_arr[0].strip() 
			sm = source_regex.match(source_spm)
			if sm:
				source_type = sm.group(1)
				source_id = sm.group(2)
				source_tn = sm.group(3)
			
			referer_spm = spm_path_arr[-2].strip() 
			rm = source_regex.match(referer_spm)
			if rm:
				referer_type = rm.group(1)
				referer_id = rm.group(2)
		'''
		pv_source = "none"
		if "pv_source" in query_dict:
			pv_source = query_dict["pv_source"]
		
		pv_gt = "none"
		if "pv_gt" in query_dict:
			pv_gt = query_dict["pv_gt"]
		pv_ngi = "none"
		if "pv_ngi" in query_dict:
			pv_ngi = query_dict["pv_ngi"]
		pv_ni = "none"
		if "pv_ni" in query_dict:
			pv_ni = query_dict["pv_ni"]
		pv_dt = "none"
		if "pv_dt" in query_dict:
			pv_dt = query_dict["pv_dt"]
		pv_di = "none"
		if "pv_di" in query_dict:
			pv_di = query_dict["pv_di"]
		'''
		fm = "none"
		if "fm" in query_dict:
			fm = query_dict["fm"]
	
		'''
		if fm == "none":
			fm ="v1_display"
		'''

		display_goods = "none"
		if "display_goods" in query_dict:
			display_goods = query_dict["display_goods"]

		referer_url = "none"
		if "referer_url" in query_dict:
			referer_url = query_dict["referer_url"]
			referer_url = unquote_plus(referer_url)
			
		#click
		click_url = "none"
		if "url" in query_dict:
			click_url = query_dict["url"]
			click_url = unquote_plus(click_url)
		click_inner = 0
		click_o = urlparse(click_url)
		click_host = str(click_o.hostname)
		if click_host.endswith("koudaitong.com"):
			click_inner = 1
		
		title = "none"
		if "title" in query_dict:
			title = query_dict["title"]
			title = unquote_plus(title)
			try:
				title_tmp = title.decode("utf8")
			except:
				title = ""
		
		network_type = "none"
		if "msg" in query_dict:
			msg = query_dict["msg"]
			msg = unquote_plus(msg)
			network_type = msg.split(":")[-1]
		
		click_id = "none"
		if "click_id" in query_dict:
			click_id = query_dict["click_id"]
			click_id = unquote_plus(click_id)
		
		click_type = "none"
		if "click_type" in query_dict:
			click_type = query_dict["click_type"]
		
		alias = "none"
		if "alias" in query_dict:
			alias = query_dict["alias"]
		
		#cookie
		arr = cookie.split(";")
		cookie_dict = {}
		for item in arr:
			if not "=" in item:
				continue
			'''
			try:
				key,value = item.strip().split("=")
			except ValueError:
				print line
				sys.exit()
			'''
			try:
				key,value = item.strip().split("=")
			except:
				continue
			cookie_dict[key] = value
		
		# cookie info
		kdt_id = "0"
		if "_kdt_id_" in cookie_dict:
			kdt_id = cookie_dict["_kdt_id_"]
		if not kdt_regex.match(kdt_id):
			kdt_id = "0"
		ut = "none"
		if "ut" in cookie_dict:
			ut = cookie_dict["ut"]
		kdt_session_id = "none"
		if "KDTSESSIONID" in cookie_dict:
			kdt_session_id = cookie_dict["KDTSESSIONID"]
		verify_weixin_openid = "none"
		if "verify_weixin_openid" in cookie_dict:
			verify_weixin_openid = cookie_dict["verify_weixin_openid"]
		
		# fans info	
		is_fans = "0"
		if "is_fans" in cookie_dict:
			is_fans = cookie_dict["is_fans"]
		fans_id = "none"
		if "fans_id" in cookie_dict:
			fans_id = cookie_dict["fans_id"]


		# user iphone
		user_os = "other"
		os_version = "other"
		machine = "other"
		if "iphone" in ua.lower():
			user_os = "iphone"
			ir = iphone_regex.match(ua)
			if ir:
				os_version = ir.group(1)
				machine = "iphone"
		elif "android" in ua.lower():
			user_os = "android"
			ar = android_regex.match(ua)
			if ar:
				os_version = ar.group(1)
				machine = ar.group(2).replace(" ","_")
				machine = machine.replace("\t","_")
		

		if "MicroMessenger".lower() in ua.lower():
			user_client = "MicroMessenger"
		else:
		 	user_client = "other"
		weixin_version = "other"
		m = client_regex.search(ua)
		if m and len(m.groups()) == 1:
			weixin_version = m.group(1) 


		#ip to region
		province = "none"
		city = "none"
		try:
			address = ip_locater.getIpAddr(string2ip(ip))
			address = address.decode("gbk").encode("utf-8")
			if "\xe7\x9c\x81" in address:
				#m = region_regex.search(address)
				#province = m.group(1)
				pos1 = address.find("\xe7\x9c\x81")
				province = address[:pos1]
				city_str = address[pos1+3:]
				
				if "\xe5\xb8\x82" in city_str:
					pos2 = city_str.find("\xe5\xb8\x82")
					city = city_str[:pos2]
				else:
					city = city_str
					if len(city.strip()) == 0:
						city = ip

			else:
				province = address
				#print "ori:" + address
				for item in plist:
					if address.startswith(item.strip()):
						province = item
						city = address[len(item):]
						break
				city = city.replace("\xe5\xb8\x82","")

				'''
				print address
				address_arr = address.split("\xe7\x9c\x81")
				province = address_arr[0]
				province = address_arr[0]
				city_str = address[pos+2:]
				if len(address_arr) > 1:
					city_str = address_arr[1]
					city_str = city_str.strip("\xe5\xb8\x82")
					city_arr = city_str.split("\xe5\xb8\x82")
					#city = city_arr[0].strip("\xca\xd0")
					city = city_arr[0]
				'''

				#print province + "\t" +city
		except:
			province = "none"
			city = "none"
		
		#output
		sort_str = "\t".join(map(str,[kdt_id,kdt_session_id,log_time]))
		user_str = "\t".join(map(str,[verify_weixin_openid,user_os,os_version,machine,user_client,weixin_version,ip,province,city,fm]))
		source_str = "\t".join(map(str,[referer_url,referer_type,referer_id,source_type,source_id,source_tn]))
		dis_str = "\t".join(map(str,[display_url,spm,display_type,display_id,display_goods,is_share,title]))
		click_str = "\t".join(map(str,[click_type,click_url,click_inner,click_id]))
		other_str = "\t".join(map(str,[network_type,scan,activity,is_fans,fans_id]))

		out_str = "\t".join([sort_str,user_str,source_str,dis_str,click_str,other_str])

		print out_str




