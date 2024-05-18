
from rest_framework.views import APIView
from rest_framework.parsers import MultiPartParser
from rest_framework.response import Response
from .tasks import verify_emails_single_task, verify_emails_task

class FileUploadView(APIView):
    parser_classes = [MultiPartParser]

    def post(self, request, *args, **kwargs):
        file_obj = request.FILES['file']
        if file_obj:
            
            path = self.handle_file(file_obj)
            # print(path)
            verify_emails_task.delay(path)
            return Response({'message': 'File received and processing started.'})

        return Response({'error': 'No file uploaded'}, status=400)


    def handle_file(self, file_obj):
        # Save file to disk and return the file path
        name = str(file_obj)
        file_path = '/home/ubuntu/Documents/GitHub/Email-Verification-Backend/emailverify/emailverify/CSV/'+name
        with open(file_path, 'wb+') as destination:
            for chunk in file_obj.chunks():
                destination.write(chunk)
        return file_path
    
class SingleEmail(APIView):

    def post(self, request,*args,**kwargs):
        email = request.data.get('email')
        verify_emails_single_task.delay(email)
        return Response({'message': 'Email Received & Processing'}) 