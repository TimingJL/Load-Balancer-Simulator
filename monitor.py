#!/usr/bin/env python
#coding=utf-8
import redis
import time

r = redis.StrictRedis(host='localhost', port=6379, db=0)
cm_list = ['cm_01','cm_02','cm_03']
#cm_list = ['cm_01']

while True:

	print 'Jobs in Queue_01: %r' % (r.get(cm_list[0]))
	print 'Jobs in Queue_02: %r' % (r.get(cm_list[1]))
	print 'Jobs in Queue_03: %r' % (r.get(cm_list[2]))
	print '\n\n'
	time.sleep(1)