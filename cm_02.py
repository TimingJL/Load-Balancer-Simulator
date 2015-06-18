#!/usr/bin/env python
import pika
import time
import thread
#from threading import Time
import numpy as np
from collections import deque
from threading import Thread
import parameter

credentials = pika.PlainCredentials(parameter.rabbitmq_username, parameter.rabbitmq_password)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=parameter.rabbitmq_host,port = 5672, virtual_host = parameter.rabbitmq_vhost, credentials = credentials))
channel = connection.channel()

queue_name = 'cm_02'
msg_queue = deque([])

channel.queue_declare(queue=queue_name, durable=True)
print ' [*] Waiting for messages. To exit press CTRL+C'


def out():
    while True:        
        if msg_queue:
            s = float(msg_queue.popleft())
            time.sleep(s)

            print '\n pop :' + str(s)
            print 'msg_queue: %r' %(msg_queue)
            print '\n\n'
            


def callback(ch, method, properties, body):
    print " [x] Received %r" % (body,)
    msg_queue.append(body)
    print ' in queue'
    print msg_queue
    print '\n\n'
    ch.basic_ack(delivery_tag = method.delivery_tag)
    #thread.start_new_thread(Threadfun, (msg_queue, 2, lock))

th = Thread(target=out)
th.start()

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue=queue_name)
channel.start_consuming()