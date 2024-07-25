
from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from .tasks import verify_emails_single_task, verify_emails_task, distribute_email_verification_tasks
from rest_framework.decorators import api_view
from celery.result import AsyncResult
import uuid

class FileUploadView(APIView):
    parser_classes = [MultiPartParser]

    def post(self, request, *args, **kwargs):
        file_obj = request.FILES['file']
        if file_obj:
            
            path = self.handle_file(file_obj)
            task_id = str(uuid.uuid4()) 
            # print(path)
            distribute_email_verification_tasks.delay(path,task_id)
            return Response({'taskid': task_id})

        return Response({'error': 'No file uploaded'}, status=400)


    def handle_file(self, file_obj):
        # Save file to disk and return the file path
        name = str(file_obj)
        random_filename = f"{uuid.uuid4()}.csv"
        file_path = '/home/sikandar/Documents/GitHub/Email-Verification-Backend/emailverify/emailverify/CSV/'+random_filename
        with open(file_path, 'wb+') as destination:
            for chunk in file_obj.chunks():
                destination.write(chunk)
        return file_path
    
class SingleEmail(APIView):

    def post(self, request,*args,**kwargs):
        email = request.data.get('email')
        task = verify_emails_single_task.apply_async(args=[email], countdown=5) 
        
        return Response({'task_id': task.id}, status=202) 



@api_view(['GET'])
def check_task_status(request, task_id):
    task_result = AsyncResult(task_id)
    if task_result.state == 'PENDING':
        return Response({'status': task_result.state}, status=200)
    elif task_result.state != 'FAILURE':
        return Response({
            'status': task_result.state,
            'result': task_result.result
        }, status=200)
    else:
        return Response({'status': task_result.state}, status=400)