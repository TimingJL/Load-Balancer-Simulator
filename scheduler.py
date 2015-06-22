#coding=utf-8
#!/usr/bin/env python
import pika
import sys
import random
import parameter
import redis
import ast

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set('count', 0)

credentials = pika.PlainCredentials(parameter.rabbitmq_username, parameter.rabbitmq_password)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=parameter.rabbitmq_host,port = 5672, virtual_host = parameter.rabbitmq_vhost, credentials = credentials))
channel = connection.channel()

waiting_time = [0.,0.,0.]

def mRR(task, cm_list):
  r.set('scheduler_mode', 'Round-Robin')
  i = int(r.get('count'))
  #i = 1 #test var
  channel.basic_publish(exchange=cm_list[i],
                  routing_key=cm_list[i],
                  body=task,
                  properties=pika.BasicProperties(
                     delivery_mode = 2, # make message persistent
                  ))
  print 'send to cm_0%r' % (i+1)
  i = (i + 1) % len(cm_list)
  r.set('count', i)


def mRandom(task, cm_list):
  r.set('scheduler_mode', 'Random')
  ran = random.randint(0,len(cm_list)-1)
  print 'send to: %r' %(cm_list[ran])
  channel.basic_publish(exchange=cm_list[ran],
                  routing_key=cm_list[ran],
                  body=task,
                  properties=pika.BasicProperties(
                     delivery_mode = 2, # make message persistent
                  ))


# Join the Shortest Queue(fewest number of jobs)
def mJSQ(task, cm_list):
  r.set('scheduler_mode', 'JSQ (Join the Shortest Queue)')
  queue_index = 0
  min_queue_length = int(r.get(cm_list[queue_index]))
  min_queue_ID = cm_list[queue_index]
  for item in cm_list:
    if int(r.get(item)) < min_queue_length:
      min_queue_ID = item
      min_queue_length = int(r.get(min_queue_ID))

  channel.basic_publish(exchange=min_queue_ID,
                routing_key=min_queue_ID,
                body=task,
                properties=pika.BasicProperties(
                    delivery_mode = 2, # make message persistent
                ))

# Join the Shortest Waiting Queue(lowest waiting time)
def mJSWQ(task, cm_list):
  r.set('scheduler_mode', 'JSWQ (Join the Shortest Waiting Queue)')
  queue_index = 0
  waiting_time[0] = float(r.get("wiq_01"))
  waiting_time[1] = float(r.get("wiq_02"))
  waiting_time[2] = float(r.get("wiq_03"))

  min_waiting_time = waiting_time[0]
  min_queue_ID = cm_list[queue_index]
  for item in range(0,3):
    if waiting_time[item] < min_waiting_time:
      min_queue_ID = cm_list[item]
      min_waiting_time = waiting_time[item]

  print 'w[1]: %r' %(waiting_time[0])
  print 'w[2]: %r' %(waiting_time[1])
  print 'w[3]: %r' %(waiting_time[2])
  print 'min: %r' %(min_waiting_time)
  print 'send to: %r\n' %(min_queue_ID)

  channel.basic_publish(exchange=min_queue_ID,
                routing_key=min_queue_ID,
                body=task,
                properties=pika.BasicProperties(
                    delivery_mode = 2, # make message persistent
                ))