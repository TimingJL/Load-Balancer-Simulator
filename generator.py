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
import datetime
from time import strftime

now = strftime("%Y%m%d-%H%M%S %P")

r = redis.StrictRedis(host='localhost', port=6379, db=0)
r.set("flag",1)
r.set("window",0)
r.set("test_timing",str(now))
for i in range(0,4000):
    r.set(i,'0')#r.set(message["ID"],message)

def time_window():
    while(True):
        time.sleep(60)# count down(second)
        window = int(r.get("window"))
        window = window + 1
        r.set("window", window)

def clock():
    time.sleep(15*60)# count down(second)
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
th2 = Thread(target=time_window)
th.start()
th2.start()

#task generator 
a, m = 1.1, 0. # shape and mode
i = 1
while r.get("flag") == '1':
    #poisson
    t = nextTime(1/0.4)

    #numpy.random.pareto
    #http://docs.scipy.org/doc/numpy/reference/generated/numpy.random.pareto.html
    # lower = 10  # the lower bound for your values
    # shape = 1   # the distribution shape parameter, also known as `a` or `alpha`
    # size = 1000 # the size of your sample (number of random values)
    # x = np.random.pareto(shape, size) + lower
    s = np.random.pareto(a, 1) + m #generate pareto job size
    s[0] = str(round(float(s[0]),1))
    if float(s[0])>30.:
        s[0] = '30.'
    time.sleep(t)#sleep poisson distribution   
    #message = {"ID":i,"job_size":s[0], "isFinished":'0', "window":r.get("window")}
    message = {"ID":str(i),"job_size":s[0], "isFinished":'0', "window":r.get("window"), "testID":now}
    channel.basic_publish(exchange='LB',
                      routing_key='',
                      body=str(message),
                      properties=pika.BasicProperties(
                         delivery_mode = 2, # make message persistent
                      ))
    print '[%r]random pareto job size: %r' % (message["ID"], message["job_size"])
    r.set("job_number", i)
    print 'str(message): %r\n' %(str(message))
    i = i+1

job_number = int(r.get("job_number"))
print "get out of while loop !!! \nwaiting for Queue Length == 0"

while (int(r.get('cm_01'))+int(r.get('cm_02'))+int(r.get('cm_03')) > 0):
    #waiting for Queue == 0
    print 'policy: %r' %(r.get("scheduler_mode"))
    print 'number of jobs: %r' % (job_number)
    print 'Jobs in Queue_01: %r' % (r.get('cm_01'))
    print 'Jobs in Queue_02: %r' % (r.get('cm_02'))
    print 'Jobs in Queue_03: %r' % (r.get('cm_03'))
    print '\n\n'
    time.sleep(1)
