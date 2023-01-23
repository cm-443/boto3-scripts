import json
import pandas as pd
import pymysql
import smtplib
import boto3
import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from smtplib import SMTP_SSL, SMTPException, SMTP

#This function queries a database, converts the data into a csv, and emails the file

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

# Sets a year var
Year = 0
today = datetime.date.today()
month = today.month
currentYear = today.year
if month >= 8:
    Year = currentYear
else:
    Year = currentYear - 1


def sso_handler(event, context):
    # Executes the mySQL query
    query = "SQL STATEMENT"
    sqlVar = Year
    cur.execute(query, sqlVar)
    output = cur.fetchall()

    # Takes the mySQL query output and transforms it into a csv file
    c_file = pd.DataFrame(output,
                          columns=['column1', 'column2', 'column3', 'column4'])
    data = c_file.to_csv('/tmp/file.csv', index=False)

    # Sends the email
    SENDER = "Sender Name <no-reply@place.com>"
    recipients = ["First Last <person@place.com>"]
    RECIPIENT = ", ".join(recipients)

    # SMTP Vars
    USERNAME_SMTP = secret_ses['example']
    PASSWORD_SMTP = secret_ses['example']
    HOST = "host"
    PORT = 465

    # Email Vars
    PATH_TO_CSV_FILE = "/tmp/file.csv"
    FILE_NAME = "file.csv"
    AWS_REGION = "region"
    SUBJECT = "Subject"
    BODY_TEXT = (f"Please see attached.")
    CHARSET = "UTF-8"

    # SMTP
    msg = MIMEMultipart()
    msg['Subject'] = SUBJECT
    msg['From'] = SENDER
    msg['To'] = RECIPIENT
    part1 = MIMEText(BODY_TEXT, 'plain')
    msg.attach(part1)

    with open(PATH_TO_CSV_FILE, 'rb') as f:
        msg.attach(MIMEApplication(f.read(), Name=FILE_NAME))

    try:
        with SMTP_SSL(HOST, PORT) as server:
            server.login(USERNAME_SMTP, PASSWORD_SMTP)
            server.sendmail(SENDER, RECIPIENT, msg.as_string())
            server.close()

    except SMTPException as e:
        print("Error: ", e)

    conn.close()

