#coding=utf-8
import csv
import redis
import ast

r = redis.StrictRedis(host='localhost', port=6379, db=0)
name = str(r.get("test_timing")) + '-' + str(r.get('scheduler_mode')) + '.csv'

#{'response_time': 5756.013859987259, 'isFinished': '1', 'out_Server_time': 1435203158.115893, 'queueing_time': 5753.3224329948425, 'window': '14', 'serviceby': 'cm_02', 'out_Queue_time': 1435203155.424466, 'job_size': 2.6894625948505357, 'ID': 2220, 'in_Queue_time': 1435197402.102033}
#message["ID"]
with open(name, 'w') as csvfile:
	fieldnames = ['ID','window', 'serviceby', 'testID', 'job_size', 'in_Queue_time', 'out_Queue_time', 'out_Server_time', 'queueing_time', 'response_time', 'isFinished']
	writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
	writer.writeheader()

	job_number = int(r.get("job_number"))
	print 'job_size: %r' %(job_number)
	for row in range(1, job_number+1):#
		#print 'row: %r' %(row)
		#print 'r.get(row): %r' %(r.get(row))
		message = ast.literal_eval(r.get(row))
		#print row
		#writer.writerow({'ID':r.get(message[str(row)])["ID"],'window':r.get(message[str(row)])["ID"]})
		#print 'message: %r' %(message)
		if message:
			writer.writerow({'ID':message["ID"], 
							'window':message["window"],
							'serviceby':message["serviceby"],
							'testID':message["testID"],
							'job_size':message["job_size"],
							'in_Queue_time':message["in_Queue_time"],
							'out_Queue_time':message["out_Queue_time"],
							'out_Server_time':message["out_Server_time"],
							'queueing_time':message["queueing_time"],
							'response_time':message["response_time"],
							'isFinished':message["isFinished"],
							})
	#print message

