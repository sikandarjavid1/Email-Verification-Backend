# tasks.py
from celery import shared_task
import csv
from emailverification.models import Email
from emailverification.verification_utils import *

@shared_task
def verify_emails_task(file_path):
    with open(file_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        results = []
        for row in reader:
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
        return results
@shared_task
def verify_emails_single_task(email):
   
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
    return results
