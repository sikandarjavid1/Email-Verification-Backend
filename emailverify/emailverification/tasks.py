# tasks.py
from celery import shared_task
import csv
from emailverification.models import Email
from emailverification.verification_utils import *
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync

from celery import shared_task, group

@shared_task
def distribute_email_verification_tasks(file_path, task_id):
    total_rows = get_total_rows_fast(file_path)
    chunks = chunk_file(file_path, 100)  # Assuming each chunk has 100 rows
    job = group(verify_emails_task.s(chunk, task_id, total_rows) for chunk in chunks)
    result = job.apply_async()
    return result


@shared_task
def verify_emails_task(chunk_data, task_id, total_rows):
    channel_layer = get_channel_layer()
    progress_increment = len(chunk_data)
    current_progress = 0

    results = []
    for row in chunk_data:
            email = row[0]
            if not verify_email_syntax(email):
                result = (email, 'Invalid syntax')
            elif is_disposable_email(email):
                result = (email, 'Disposable email')
            elif not verify_domain(email):
                result = (email, 'Invalid domain')
            elif not verify_smtp(email):
                result = (email, 'SMTP check failed')
            else:
                result = (email, 'Verified')
            results.append(result)
            Email.objects.create(email=email, verified=(result[1] == 'Verified'))

            current_progress += progress_increment
            progress_percentage = calculate_progress(current_progress, total_rows)

    async_to_sync(channel_layer.group_send)(
        f'task_{task_id}',
        {
            'type': 'task_status_update',
            'data': {
                'status': 'IN_PROGRESS',
                'progress': calculate_progress(chunk_data),
            }
        }
    )
    return results
    
def get_total_rows_fast(file_path):
    """Calculate the total number of rows in a CSV file quickly and efficiently."""
    with open(file_path, 'r', encoding='utf-8') as file:
        return sum(1 for _ in file)
def calculate_progress(current, total):
    """Calculate the progress percentage."""
    return (current / total) * 100
  
@shared_task(bind=True)
def verify_emails_single_task(self, email):
    channel_layer = get_channel_layer()
    task_group_name = f'task_{self.request.id}'
   

    results = []
    if not verify_email_syntax(email):
        result = (email, 'Invalid syntax')
    elif is_disposable_email(email):
        result = (email, 'Disposable email')
    elif not verify_domain(email):
        result = (email, 'Invalid domain')
    elif not verify_smtp(email):
        result = (email, 'SMTP check failed')
    else:
        result = (email, 'Verified')
    results.append(result)
    Email.objects.create(email=email, verified=(result[1] == 'Verified'))
      # Send progress update
    async_to_sync(channel_layer.group_send)(
        task_group_name,
        {
            'type': 'task_status_update',
            'data': {
                'message': 'IN_PROGRESS',
                'progress': '50%',
                'current': 1,
                'total': 1
            }
        }
    )
    time.sleep(2)  # Simulate time taken to verify email

    # Send final result
    async_to_sync(channel_layer.group_send)(
        task_group_name,
        {
            'type': 'task_status_update',
            'data': {
                'message': 'SUCCESS',
                'result': results
            }
        }
    )
    return results
