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



credentials = pika.PlainCredentials(parameter.rabbitmq_username, parameter.rabbitmq_password)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=parameter.rabbitmq_host,port = 5672, virtual_host = parameter.rabbitmq_vhost, credentials = credentials))
channel = connection.channel()

queue_name = 'cm_03'
msg_queue = deque([])

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set(queue_name, 0)

channel.queue_declare(queue=queue_name, durable=True)
print ' [*] Waiting for messages. To exit press CTRL+C'


def out():
    while True:        
        if msg_queue:          
            s = float(msg_queue.popleft()[0])
            print ' pop'
            print 'msg_queue: %r' %(msg_queue)
            r.set(queue_name, len(msg_queue))  
            print 'queue length: %r' %(r.get(queue_name))
            print '\n\n'
            
            time.sleep(s)
 


def callback(ch, method, properties, body):
    msg_queue.append(body)
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