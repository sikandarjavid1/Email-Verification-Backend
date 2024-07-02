# verification_utils.py
from django.core.validators import validate_email
from django.core.exceptions import ValidationError
import dns.resolver
from smtplib import SMTPServerDisconnected, SMTPConnectError, SMTPResponseException 
import smtplib
import socket
import logging
import time
import csv

def verify_email_syntax(email):
    try:
        validate_email(email)
        return True
    except ValidationError:
        return False

def verify_domain(email):
    domain = email.split('@')[1]
    try:
        records = dns.resolver.resolve(domain, 'MX')
        return True if records else False
    except dns.resolver.NoAnswer:
        return False
    except dns.resolver.NXDOMAIN:
        return False

def is_disposable_email(email):
    disposable_domains = ['tempmail.com', 'mailinator.com', '10minutemail.com']  # Add more as needed
    domain = email.split('@')[1]
    return domain in disposable_domains

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
# smtplib.set_debuglevel(1)

def verify_smtp(email):
    domain = email.split('@')[1]
    try:
        # Resolve MX records
        records = dns.resolver.resolve(domain, 'MX')
        mx_record = records[0].exchange.to_text()
        logger.info(f"Resolved MX record for {domain}: {mx_record}")

        # Connect to the SMTP server
        server = smtplib.SMTP(host=mx_record, port=25, timeout=10)
        server.ehlo_or_helo_if_needed()
        logger.info("Connected to SMTP server")

        # Start TLS for security
        server.starttls()
        server.ehlo_or_helo_if_needed()
        logger.info("Started TLS")

        # Verify the email address
        server.mail('sikandar@ayraxs.co')
        code, message = server.rcpt(email)
        server.quit()

        if code == 250:
            logger.info(f"SMTP verification successful for {email}")
            return True
        else:
            logger.warning(f"SMTP verification failed for {email} with code: {code}")
            return False

    except dns.resolver.NoAnswer:
        logger.error(f"No MX record found for domain {domain}")
    except dns.resolver.NXDOMAIN:
        logger.error(f"Domain {domain} does not exist")
    except dns.exception.Timeout:
        logger.error(f"DNS query for domain {domain} timed out")
    except smtplib.SMTPConnectError as e:
        logger.error(f"SMTP connection error for {email}: {str(e)}")
    except smtplib.SMTPServerDisconnected:
        logger.error(f"SMTP server unexpectedly disconnected")
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error for {email}: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")

    return False




def chunk_file(file_path, chunk_size=100):
    """ Yield successive chunks from file_path."""
    with open(file_path, 'r', newline='', encoding='utf-8') as file:
        reader = csv.reader(file)
        chunk = []
        for i, row in enumerate(reader):
            if (i % chunk_size == 0 and i > 0):
                yield chunk
                chunk = []
            chunk.append(row)
        if chunk:
            yield chunk

