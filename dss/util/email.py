import boto3
from botocore.exceptions import ClientError

# The character encoding for the email.
CHARSET = "UTF-8"

# Create a new SES resource and specify a region.
client = boto3.client('ses')

def send_email(sender: str, to: str, subject: str, html: str, text: str) -> str:
    try:
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
    # Display an error if something goes wrong.
    except ClientError as e:
        return e.response['Error']['Message']
    else:
        return "Email sent! Message ID: {}".format(response['ResponseMetadata']['RequestId'])

def send_checkout_success_email(sender: str, to: str, bucket: str, location: str):
    subject = "Bundle checkout complete"

    text = "Hello, your checkout request has been processed. Your files are available at bucket {} location {}.".\
        format(bucket, location)

    html = """<html>
       <head></head>
       <body>
         <h1>Hello,</h1>
         <p>
            Your checkout request has been processed.
             Your files are available at bucket <strong>{}</strong> location <strong>{}</strong>.
         </p>
       </body>
       </html>
       """.format(bucket, location)
    return send_email(sender, to, subject, html, text)


def send_checkout_failure_email(sender: str, to: str, cause: str):
    subject = "Bundle checkout failed"

    text = "Hello, your checkout request has failed due to {}.".format(cause)

    html = """<html>
       <head></head>
       <body>
         <h1>Hello,</h1>
         <p>Your checkout request has failed due to <strong>{}</strong>.</p>
       </body>
       </html>
                   """.format(cause)
    return send_email(sender, to, subject, html, text)
