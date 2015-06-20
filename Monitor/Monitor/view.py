from django.http import HttpResponse
import redis
 
def hello(request):
	return HttpResponse("Hello world ! ")


def count(request):
	r = redis.StrictRedis(host='localhost', port=6379, db=0)
	i = r.get('count')
	string = "count hi"
	return HttpResponse(i)
	#return HttpResponse("count")
