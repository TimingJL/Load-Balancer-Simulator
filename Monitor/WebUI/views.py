# Create your views here.

from django import HttpResponse
import redis

def hello(request):
     return HttpResponse("Hello world ! ")
