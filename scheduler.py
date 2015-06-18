#coding=utf-8
#!/usr/bin/env python
import pika
import sys
import random
import parameter
import redis

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set('count', 0)

credentials = pika.PlainCredentials(parameter.rabbitmq_username, parameter.rabbitmq_password)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=parameter.rabbitmq_host,port = 5672, virtual_host = parameter.rabbitmq_vhost, credentials = credentials))
channel = connection.channel()

def mRR(task, cm_list):
  i = int(r.get('count'))
  #i = 1 #test var
  channel.basic_publish(exchange=cm_list[i],
                  routing_key=cm_list[i],
                  body=task,
                  properties=pika.BasicProperties(
                     delivery_mode = 2, # make message persistent
                  ))
  print i
  i = (i + 1) % len(cm_list)  
  r.set('count', i)



def mRandom(task, cm_list):
	r = random.randint(0,len(cm_list)-1)
	
	channel.basic_publish(exchange=cm_list[r],
                  routing_key=cm_list[r],
                  body=task,
                  properties=pika.BasicProperties(
                     delivery_mode = 2, # make message persistent
                  ))