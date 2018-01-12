import boto3
from botocore.exceptions import ClientError

# The character encoding for the email.
CHARSET = "UTF-8"
SUCCESS_SUBJECT = "Bundle checkout complete"
FAILURE_SUBJECT = "Bundle checkout failed"

# Create a new SES resource
client = boto3.client('ses')

def send_email(sender: str, to: str, subject: str, html: str, text: str) -> str:
    # Provide the contents of the email.
    response = client.send_email(
        Destination={
            'ToAddresses': [
                to
            ],
        },
        Message={
            'Body': {
                'Html': {
                    'Charset': CHARSET,
                    'Data': html,
                },
                'Text': {
                    'Charset': CHARSET,
                    'Data': text,
                },
            },
            'Subject': {
                'Charset': CHARSET,
                'Data': subject,
            },
        },
        Source=sender,
    )
    return "Email sent! Message ID: {}".format(response['ResponseMetadata']['RequestId'])

def send_checkout_success_email(sender: str, to: str, bucket: str, location: str):
    text = "Hello, your checkout request has been processed. Your files are available at bucket {} location {}.".\
        format(bucket, location)

    html = """<html>
       <head></head>
       <body>
         <h1>Hello,</h1>
         <p>
            Your checkout request has been processed.
             Your files are available at <strong>s3://{}/{}</strong>
         </p>
       </body>
       </html>
       """.format(bucket, location)
    return send_email(sender, to, SUCCESS_SUBJECT, html, text)


def send_checkout_failure_email(sender: str, to: str, cause: str):
    text = "Hello, your checkout request has failed due to {}.".format(cause)
    html = """<html>
       <head></head>
       <body>
         <h1>Hello,</h1>
         <p>Your checkout request has failed due to <strong>{}</strong>.</p>
       </body>
       </html>
                   """.format(cause)
    return send_email(sender, to, FAILURE_SUBJECT, html, text)
