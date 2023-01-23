import json
import time
import pymysql
import boto3
from jira import JIRA
from smtplib import SMTP_SSL, SMTPException, SMTP
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from boto3.dynamodb.conditions import Key, Attr


#This function evaluates a mysql db, emails an alert, and creates a jira task if a threshold is exceeded 
#a dynamodb entry is also added with a timestamp and evaluated to ensure multiple jira tickets are not created

# Obtain Jira API Token from secrets manager
client = boto3.client('secretsmanager')
response = client.get_secret_value(
    SecretId='path/to/secret')
secret_jira = json.loads(response['SecretString'])

# Obtain SMTP login info from secrets manager
client = boto3.client('secretsmanager')
response = client.get_secret_value(
    SecretId='path/to/secret')
secret_ses = json.loads(response['SecretString'])

# Obtain database login info from secrets manager
client = boto3.client('secretsmanager')
response = client.get_secret_value(
    SecretId='path/to/secret')
secret_dict = json.loads(response['SecretString'])


# Connect MySQL database
conn = pymysql.connect(
    #Removed for security, examples are included below
    #example=secret_dict['example'],
    #example=secret_dict['example'],
    #example=secret_dict['example'],
    #example=secret_dict['example'],
)
cur = conn.cursor()

def analytics_handler(event, context):

    # SQL query to run
    cur.execute("SQL STATEMENT")
    output = cur.fetchall()
    # Changes the result to an int
    sql_result = []
    for i, in output:
        sql_result.append(i)
    data = int(sql_result[0])

    # Flow control for SQL result
    if data > 500:

        # Queries dynamodb to check if an email or ticket was sent out in the past 24 hours
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('table-name')
        response = table.scan(AttributesToGet=['time_stamp'])
        items = response['Items']
        x = items[0]
        time_of_last_write = x["time_stamp"]

        # Evaluates the time difference
        current_time = int(float(time.time()))
        time_diff = current_time - time_of_last_write
        if time_diff > 86400:

            # Sends the email
            SENDER = "Sender Name <no-reply@place.com>"
            recipients = ["name@place.com", "nametwo@place.com"]
            RECIPIENT = ", ".join(recipients)

            # SMTP
            USERNAME_SMTP = secret_ses['example']
            PASSWORD_SMTP = secret_ses['example']
            HOST = "host"
            PORT = 465

            # Boto3 SES
            AWS_REGION = "region"
            SUBJECT = "Subject"
            BODY_TEXT = (f"html error, {data} sent.")
            BODY_HTML = f""" <html>
                              <head>
                              </head>
                              <body>
                              <html><body style="font: 16px/1 Georgia, Serif;">
                              <h3>There are currently {data} points.</h3>
                              <p> Please take action.</p> </body> </html>"""

            CHARSET = "UTF-8"

            # SMTP
            msg = MIMEMultipart('alternative')
            msg['Subject'] = SUBJECT
            msg['From'] = SENDER
            msg['To'] = RECIPIENT

            part1 = MIMEText(BODY_TEXT, 'plain')
            part2 = MIMEText(BODY_HTML, 'html')

            msg.attach(part1)
            msg.attach(part2)

            try:
                with SMTP_SSL(HOST, PORT) as server:
                    server.login(USERNAME_SMTP, PASSWORD_SMTP)
                    server.sendmail(SENDER, RECIPIENT, msg.as_string())
                    server.close()

            except SMTPException as e:
                print("Error: ", e)

            # Creates a Jira ticket
            jira_connection = JIRA(
                basic_auth=('email', secret_jira['example']),
                server="jira server"
            )
            test_data = {
                "project": {"key": "keyName"},
                "summary": "Ticket Name",
                "description": "Please take action",
                "issuetype": {"name": "Task"}
            }
            new_issue = jira_connection.create_issue(fields=test_data)

            #Updates Dynanmodb with a new time stamp
            var = int(float(time.time()))
            table.update_item(
                Key={
                    'key': 'value',
                },
                UpdateExpression="set time_stamp = :g",
                ExpressionAttributeValues={
                    ':g': var
                },
                ReturnValues="UPDATED_NEW"
            )

            return "Threshold exceeded email and jira ticket sent."
        else:
            return "No alert required, ticket already created."
    else:
        return "No alert required, threshold not exceed."






