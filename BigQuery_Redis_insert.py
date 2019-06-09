#!/usr/bin/env python

'''
@author: Linda Li

# Project: 
# This core function reads from source BigQuery table/view and inserts records into Redis #database
# The running environment is compatible with Python 2.7.9 and Python 3.x
# Increase the execute point row count to 15000
# The configure file is specific to your directory structures, please change it accordinly
# A sample configuration file is attached for reference.
'''

from __future__ import print_function
from __future__ import division
from builtins import int
import redis
import json
from datetime import datetime
import logging
import google.cloud.bigquery as bq
from configparser import ConfigParser


EXEC_POINT=15000

logging.basicConfig(filename="/apps/tmp/insert_log_"+str(datetime.now().strftime('%Y%m%d_%H%M'))+".log", level=logging.INFO)


def read_bq(connection_json, project, sql):    
    
	client = bq.Client.from_service_account_json(connection_json, project=project)
    
   	 
	results=client.query(sql).result()
   	 
	return results


def redis_load(_host, _port, _db, _password, connection_json, project, sql):
    
   
	t1=datetime.now()
 	 
	logging.info("start time is {}".format(t1))
 
    
	redisClient=redis.StrictRedis(host=_host,port=_port, db=_db, password=_password)
   
    
	pipe=redisClient.pipeline()
    
    
	cnt=total_row_cnt=0
    
   	 
	for row in read_bq(connection_json, project, sql): # API request - fetches results
 	 
		dic_v = json.loads(row['json_attributes'])
		cnt=cnt+1
    	total_row_cnt=total_row_cnt+1
   	 
    	                  	 
    	hash_key=row['key_id'] 	 
   	 
    	logging.info(hash_key+','+ str(dic_v)) 
   	 
    	for (subkey, subvalue) in dic_v.items():
     	 
      	 
        	pipe.hset(hash_cguid, subkey, subvalue)
       	 
        	pipe.expire(hash_cguid, str(int(row['ttl'])*60*60*24)) # TTL is Redis Time to Live for the key, in seconds.
    		if cnt==EXEC_POINT:
				pipe.execute()       	 
           	 
            	#print 'command buffers complete'+str(cnt)
           	 
            	cnt=0
    
	pipe.execute()
   
	logging.info("total number of rows is "+str(total_row_cnt))
    
	 
	t2=datetime.now()
    
	logging.info("end time is "+str(t2))
    
 
	logging.info("total time "+ str((t2-t1).total_seconds()))
   	 

if __name__=="__main__":
 
    
	config=ConfigParser()
	config.read("/apps/home/config/bigQuery_Redis_config.ini")
    
	get_host=config.get('bigQuery_Redis_config','host')
	get_port=config.get('bigQuery_Redis_config','port')
	get_db=config.get('bigQuery_Redis_config','db')
	get_pass=config.get('bigQuery_Redis_config','password')
	get_connection_json=config.get('bigQuery_Redis_config','connection_json')
	get_project=config.get('bigQuery_Redis_config','project')
	get_sql=config.get('bigQuery_Redis_config','sql')
       	 
	redis_load(get_host, get_port, get_db, get_pass, get_connection_json, get_project, get_sql)
    
	

