#!/usr/bin/env python
import pika
import time
import thread
#from threading import Time
import numpy as np
from collections import deque
from threading import Thread
import parameter
import redis
import ast


credentials = pika.PlainCredentials(parameter.rabbitmq_username, parameter.rabbitmq_password)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=parameter.rabbitmq_host,port = 5672, virtual_host = parameter.rabbitmq_vhost, credentials = credentials))
channel = connection.channel()

queue_name = 'cm_02'
msg_queue = deque([])

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set(queue_name, 0)

channel.queue_declare(queue=queue_name, durable=True)
print ' [*] Waiting for messages. To exit press CTRL+C'

#receive jobs from queue and output jobs from server
def out():
    while True:
        if msg_queue:
            message = ast.literal_eval(msg_queue.popleft())
            #s = float(msg_queue.popleft()[0])
            out_Queue_time = time.time()

            r.set(queue_name, len(msg_queue))#log queue length
            
            time.sleep(message["job_size"])#service time

            out_Server_time = time.time()
            message.setdefault("out_Queue_time",out_Queue_time)
            message.setdefault("out_Server_time",out_Server_time)
            queueing_time = float(out_Queue_time) - float(message["in_Queue_time"])
            message.setdefault("queueing_time",queueing_time)
            response_time = float(out_Server_time) - float(message["in_Queue_time"])
            message.setdefault("response_time",response_time)
            r.set(message["ID"],message)

            print ' pop'
            print 'out message: %r' % (r.get(message["ID"]))
            print 'remain in msg_queue: %r' %(msg_queue)
            print 'queue length: %r' %(r.get(queue_name))
            print '\n\n'
 

#receive jobs from load balancer
def callback(ch, method, properties, body):     
    message = ast.literal_eval(body)
    in_Queue_time = time.time()
    message.setdefault("in_Queue_time", in_Queue_time)
    msg_queue.append(str(message))

    ch.basic_ack(delivery_tag = method.delivery_tag)
    r.set(queue_name, len(msg_queue))

    print ' in queue'
    print 'msg_queue: %r' %(msg_queue)
    print 'queue length: %r' %(r.get(queue_name))
    print '\n\n'


th = Thread(target=out)
th.start()

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue=queue_name)
channel.start_consuming()