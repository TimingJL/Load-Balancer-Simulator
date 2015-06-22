#coding=utf-8
import pika
import time
import sys

import math
import random
import time
import parameter
import numpy as np
import json
import redis
import thread
from threading import Thread
import ast
import csv 

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set("flag",1)

def clock():
    time.sleep(10)# count down(second)
    r.set("flag",0)

#Generate Random Timings for a Poisson Process
#http://preshing.com/20111007/how-to-generate-random-timings-for-a-poisson-process/
def nextTime(rateParameter):
    return -math.log(1.0 - random.random()) / rateParameter

#to establish a connection with RabbitMQ server
credentials = pika.PlainCredentials(parameter.rabbitmq_username, parameter.rabbitmq_password)
connection = pika.BlockingConnection(pika.ConnectionParameters(host=parameter.rabbitmq_host,
																port = 5672, 
																virtual_host = parameter.rabbitmq_vhost, 
																credentials = credentials))
channel = connection.channel()

th = Thread(target=clock)
th.start()

#task generator 
a, m = 1., 1. # shape and mode
i = 1
while r.get("flag") == '1':
    #poisson
    t = nextTime(1/0.4)

    #numpy.random.pareto
    #http://docs.scipy.org/doc/numpy/reference/generated/numpy.random.pareto.html
    s = np.random.pareto(a, 1) + m #generate pareto job size
    time.sleep(t)#sleep poisson distribution
    message = {"ID":i,"job_size":s[0], "isFinished":'0'}
    channel.basic_publish(exchange='LB',
                      routing_key='',
                      body=str(message))
    print '[%r]random pareto job size: %r' % (message["ID"], message["job_size"])
    r.set("job_number", i)
    print 'str(message): %r\n' %(str(message))
    i = i+1

# job_number = int(r.get("job_number"))
# print "get out of while loop !!! \nwaiting for Queue Length == 0"

# while (int(r.get('cm_01'))+int(r.get('cm_02'))+int(r.get('cm_03')) > 0):
#     #waiting for Queue == 0
#     print 'Jobs in Queue_01: %r' % (r.get('cm_01'))
#     print 'Jobs in Queue_02: %r' % (r.get('cm_02'))
#     print 'Jobs in Queue_03: %r' % (r.get('cm_03'))
#     print '\n\n'
#     time.sleep(1)

# unfinished = True
# while unfinished:
#     finished_job = 0
#     for ID in range(1, job_number+1):
#         message_log = ast.literal_eval(r.get(ID))
#         finished_job = finished_job + int(message_log["isFinished"])
#     if(finished_job == job_number):
#         unfinished = False
#     print "unfinished, finished job = %r,  job_number = %r" % (finished_job,job_number)

# response_time = 0.0
# queueing_time = 0.0
# for ID in range(1, job_number+1):
#     message_log = ast.literal_eval(r.get(ID))
#     response_time = response_time + message_log["response_time"]
#     queueing_time = queueing_time + message_log["queueing_time"]


# print "Scheduling Mode:%r" % (r.get("scheduler_mode"))
# print "Number of Jobs:\t%r" % (job_number)
# print "Response Time:\t%r" % (response_time/job_number)
# print "Queueing Time:\t%r" % (queueing_time/job_number)
# print "\n"