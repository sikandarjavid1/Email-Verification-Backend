# tasks.py
from celery import shared_task,chord,chain
import csv
from emailverification.models import Email
from emailverification.verification_utils import *
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
import csv
import os
import uuid


from celery import shared_task, group

# Configure logging
logging.basicConfig(level=logging.DEBUG)

@shared_task
def distribute_email_verification_tasks(file_path, task_id):
    total_rows = get_total_rows(file_path)
    logging.debug(total_rows)
    if total_rows < 100:
        # Process all emails in a single task if less than 100 emails
        chunk = list(chunk_file(file_path, chunk_size=total_rows))
        job = chain(
            verify_emails_task.s(chunk[0], task_id, total_rows),
            finalize_verification_task.s(task_id, total_rows)
        )
        result = job.apply_async()
    else:
        # Create chunks and process in parallel if more than 100 emails
        chunks = list(chunk_file(file_path, chunk_size=100))
        job = chord(
            group(verify_emails_task.s(chunk, task_id, total_rows) for chunk in chunks),
            finalize_verification_task.s(task_id, total_rows)
        )
        result = job.apply_async()
    
    return result


@shared_task
def verify_emails_task(chunk_data, task_id, total_rows):
    channel_layer = get_channel_layer()
    progress_increment = len(chunk_data)
    current_progress = 0

    results = []
    for row in chunk_data:
        email = row[0] if len(row) > 0 else None
        if email:
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

            current_progress += 1
            progress_percentage = calculate_progress(current_progress, total_rows)

            async_to_sync(channel_layer.group_send)(
                f'task_{task_id}',
                {
                    'type': 'task_status_update',
                    'data': {
                        'message': 'IN_PROGRESS',
                        'progress': progress_percentage,
                        'current': current_progress,
                            'total': total_rows
                    }
                }
            )
    return results
    
def get_total_rows(file_path):
    """Calculate the total number of non-empty rows in a CSV file."""
    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        return sum(1 for row in reader if row and row[0].strip())

def calculate_progress(processed, total):
    """Calculate the progress percentage."""
    if total == 0:
        return 0
    return (processed / total) * 100
  
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

@shared_task
def finalize_verification_task(results, task_id, total_rows):
    channel_layer = get_channel_layer()
    logging.debug(f"Results: {results}")
    # Generate a unique filename for the results CSV
    result_filename = f"results_{uuid.uuid4()}.csv"
    result_filepath = os.path.join('/home/sikandar/Documents/GitHub/Email-Verification-Backend/emailverify/emailverify/results', result_filename)
    
      # Consolidate results and write to the new CSV file
    with open(result_filepath, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Email', 'Status'])  # Write the header
        for chunk_result in results:
            logger.debug(f"Chunk Result: {chunk_result}")  # Log each chunk result
            for result in chunk_result:
                logger.debug(f"Result Item: {result}")  # Log each result item
                try:
                    email, status = result
                    writer.writerow([email, status])
                except ValueError as e:
                    logger.error(f"Error unpacking result: {result} - {e}")

    async_to_sync(channel_layer.group_send)(
                    f'task_{task_id}',
                    {
                        'type': 'task_status_update',
                        'data': {
                            'message': 'COMPLETE',
                            'progress': "100%",
                            'current': total_rows,
                            'total': total_rows,
                            'result_file': result_filename
                        }
                    }
                )