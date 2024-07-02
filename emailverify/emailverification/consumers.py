import json
from channels.generic.websocket import AsyncWebsocketConsumer
import logging

logger = logging.getLogger(__name__)

class TaskStatusConsumer(AsyncWebsocketConsumer):
    async def connect(self):
        self.task_id = self.scope['url_route']['kwargs']['task_id']
        self.group_name = f'task_{self.task_id}'
        
        logger.debug(f'Connecting to group: {self.group_name}')
        
        # Join task group
        await self.channel_layer.group_add(
            self.group_name,
            self.channel_name
        )

        await self.accept()

    async def disconnect(self, close_code):
        logger.debug(f'Disconnecting from group: {self.group_name}')
        
        # Leave task group
        await self.channel_layer.group_discard(
            self.group_name,
            self.channel_name
        )

    # Receive message from WebSocket
    async def receive(self, text_data):
        pass
    # Receive message from task group
    async def task_status_update(self, event):
        # Handles messages with 'type' set to 'task_status_update'.
        data = event['data']
        # Send message back to WebSocket
        await self.send(json.dumps(data))
