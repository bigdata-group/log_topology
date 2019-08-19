#!/usr/local/bin/python
#coding=utf-8
# Author: 568190317@qq.com liumeng

import os,redis,sys

MYSQL_PATH = "/usr/local/bin/mysql"
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PWD = "root"
MYSQL_DB = "liumeng"
SQL_FILE = "./update.sql"

REDIS_HOST = "localhost"
REDIS_PORT = "6379"
REDIS_DB = "1"


if __name__ == "__main__":

	cmd = MYSQL_PATH + " -h" + MYSQL_HOST \
		+ " -u" + MYSQL_USER \
		+ " -p" + MYSQL_USER \
		+ " -D" + MYSQL_DB \
		+ " <"  + SQL_FILE
	print >> sys.stderr,cmd
	os.system(cmd)

	r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB)
	r.flushdb()


