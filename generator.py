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

#task generator 
a, m = 1., 1. # shape and mode
i = 1
while True:
    #poisson
    t = nextTime(1/0.4)

    #numpy.random.pareto
    #http://docs.scipy.org/doc/numpy/reference/generated/numpy.random.pareto.html
    s = np.random.pareto(a, 1) + m #generate pareto job size
    time.sleep(t)#sleep poisson distribution
    message = {"ID":i,"job_size":s[0]}
    channel.basic_publish(exchange='LB',
                      routing_key='',
                      body=str(message))
    print '[%r]random pareto job size: %r' % (message["ID"], message["job_size"])
    print 'str(message): %r\n' %(str(message))
    i = i+1