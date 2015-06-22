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

queue_name = 'cm_01'
#content_waiting_in_queue = 'wiq_02'
#waiting_time = 0.0
msg_queue = deque([])

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set(queue_name, 0)#clear queue length
#r.set(content_waiting_in_queue, waiting_time)

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
            
            time.sleep(float(message["job_size"]))#service time
            #caculate waiting time
            # waiting_time = 0.0
            # if msg_queue:
            #     for item in msg_queue:
            #         msg = ast.literal_eval(item)
            #         waiting_time = waiting_time + float(msg["job_size"])
            #     r.set(content_waiting_in_queue, waiting_time)

            out_Server_time = time.time()
            message.setdefault("out_Queue_time",out_Queue_time)
            message.setdefault("out_Server_time",out_Server_time)
            queueing_time = float(out_Queue_time) - float(message["in_Queue_time"])

            message.setdefault("queueing_time",queueing_time)
            response_time = float(out_Server_time) - float(message["in_Queue_time"])
            message.setdefault("response_time",response_time)
            message["isFinished"] = '1'
            r.set(message["ID"],message)

            print ' pop'
            print 'ID: %r' %(message["ID"])
            # print 'out message: %r' % (r.get(message["ID"]))
            #print 'remain in msg_queue: %r' %(msg_queue)
            print 'queue length: %r' %(r.get(queue_name))
            # print '\n\n'
 

#receive jobs from load balancer
def callback(ch, method, properties, body):     
    message = ast.literal_eval(body)
    in_Queue_time = time.time()
    message.setdefault("in_Queue_time", in_Queue_time)
    message.setdefault("serviceby", queue_name)
    msg_queue.append(str(message))

    #waiting_time = 0.0
    #for i in range(1,101):
    try:
        #ch.basic_ack(delivery_tag = method.delivery_tag)
        ch.basic_ack(method.delivery_tag)
        print 'ack==================================='
        print 'delivery_tag: %r' %(method.delivery_tag)
    except Exception as err:
        print 'Exception: %r==========================================' %(Exception)
        print 'error: %r' %err
    r.set(queue_name, len(msg_queue))

    #caculate waiting time
    # if msg_queue:
    #     for item in msg_queue:
    #         msg = ast.literal_eval(item)
    #         waiting_time = waiting_time + float(msg["job_size"])
    #r.set(content_waiting_in_queue, waiting_time)
    print ' in queue'
    #print 'waiting_time: %r' %(r.get(content_waiting_in_queue))
    # print 'msg_queue: %r' %(msg_queue)
    print 'queue length: %r' %(r.get(queue_name))
    # print '\n\n'


th = Thread(target=out)
#th2 = Thread(target=establish_a_connection)
th.start()
#th2.start()

channel.basic_qos(prefetch_count=2000)
channel.basic_consume(callback, queue=queue_name)
channel.start_consuming()