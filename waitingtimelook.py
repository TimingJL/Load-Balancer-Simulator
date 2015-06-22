#!/usr/bin/env python
import time
import redis

r = redis.StrictRedis(host='localhost', port=6379, db=0)
waiting_time = [0.,0.,0.]
while True:

	waiting_time[0] = float(r.get("wiq_01"))
	waiting_time[1] = float(r.get("wiq_02"))
	waiting_time[2] = float(r.get("wiq_03"))

	print 'w1: %r' %(waiting_time[0])
	print 'w2: %r' %(waiting_time[1])
	print 'w3: %r' %(waiting_time[2])
	print '\n\n'
	time.sleep(1)