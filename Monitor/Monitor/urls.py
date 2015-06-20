from django.conf.urls import patterns, include, url
from Monitor.view import hello
from Monitor.view import count
# Uncomment the next two lines to enable the admin:
# from django.contrib import admin
# admin.autodiscover()

urlpatterns = patterns('',
    ('^hello/$', hello),
    ('^count/$',count),
    ('^count/$',hello),
    # Examples:
    # url(r'^$', 'Monitor.views.home', name='home'),
    # url(r'^Monitor/', include('Monitor.foo.urls')),

    # Uncomment the admin/doc line below to enable admin documentation:
    # url(r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    # url(r'^admin/', include(admin.site.urls)),
)
