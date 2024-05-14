# verification_utils.py
from django.core.validators import validate_email
from django.core.exceptions import ValidationError
import dns.resolver
from smtplib import SMTPServerDisconnected, SMTPConnectError, SMTPResponseException 
import smtplib
import socket
import logging

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

def verify_smtp(email):
    domain = email.split('@')[1]
    try:
        records = dns.resolver.resolve(domain, 'MX')
        mx_record = records[0].exchange.to_text()
        server = smtplib.SMTP(host=mx_record, timeout=10)
        server.starttls()  # Upgrade to secure connection if possible
        server.helo('smtp.titan.email')
        server.mail('sikandar@ayraxs.com')
        code, message = server.rcpt(str(email))
        return True if code == 250 else False
    except SMTPServerDisconnected:
        logger.error(f"SMTP server disconnected unexpectedly: {email}")
        return False
    except SMTPConnectError:
        logger.error(f"Connection error while contacting SMTP server: {email}")
        return False
    except SMTPResponseException as e:
        logger.error(f"SMTP server response error {e.smtp_code}: {email}")
        return False
    except Exception as e:
        logger.error(f"SMTP verification failed for {email}: {str(e)}")
        return False
    finally:
        try:
            server.quit()
        except Exception:
            pass
