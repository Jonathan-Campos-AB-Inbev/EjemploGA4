from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import (Mail, Attachment, FileContent, FileName, FileType, Disposition)
from api.utils.mange_enviroment_varible import get_variable

def send_email(message):
    message = Mail(
        from_email='srendon@direcly.com',
        to_emails=["srendon@direcly.com"],
        subject='Notification Process - Api Google ADS - AB inBev',
        html_content=f'<strong>{message}</strong>')

    sg = SendGridAPIClient(get_variable('SENDGRID_API_KEY'))
    response = sg.send(message)
    return response.status_code
