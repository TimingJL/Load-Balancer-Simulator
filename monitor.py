#!/usr/bin/env python
#coding=utf-8
import redis
import time
import csv
import ast

r = redis.StrictRedis(host='localhost', port=6379, db=0)
cm_list = ['cm_01','cm_02','cm_03']
#cm_list = ['cm_01']

# while True:

# 	print 'Jobs in Queue_01: %r' % (r.get(cm_list[0]))
# 	print 'Jobs in Queue_02: %r' % (r.get(cm_list[1]))
# 	print 'Jobs in Queue_03: %r' % (r.get(cm_list[2]))
# 	print '\n\n'
# 	time.sleep(1)

# writer = csv.writer(open('dict.csv', 'w'))

# job_number = int(r.get("job_number"))
# for ID in range(1, job_number+1):
# 	message_log = ast.literal_eval(r.get(ID))
# 	for key, value in message_log.items():
# 		writer.writerow([key, value])

job_number = int(r.get("job_number"))
response_time = 0.0
queueing_time = 0.0
for ID in range(1, job_number+1):
    message_log = ast.literal_eval(r.get(ID))
    response_time = response_time + message_log["response_time"]
    queueing_time = queueing_time + message_log["queueing_time"]

print "\n"
print "Scheduling Mode:%r" % (r.get("scheduler_mode"))
print "Number of Jobs:\t%r" % (job_number)
print "Response Time:\t%r" % (response_time/job_number)
print "Queueing Time:\t%r" % (queueing_time/job_number)
print "\n"