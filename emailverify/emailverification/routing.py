
from django.urls import re_path
from emailverification.consumers import TaskStatusConsumer

websocket_urlpatterns = [
    re_path(r'^ws/task_status/(?P<task_id>[\w-]+)/$', TaskStatusConsumer.as_asgi()),
]
