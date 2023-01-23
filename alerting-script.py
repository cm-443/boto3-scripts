import json
import pymysql
import boto3
from smtplib import SMTP_SSL, SMTPException, SMTP
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

#This function evaluates a db every 3 minutes and send out an alert if needed
#Note the db connection is placed outside of the handler so it can reuse the same db connection

# Obtain SMTP login info from secrets manager
client = boto3.client('secretsmanager')
response = client.get_secret_value(
    SecretId='path/to/secret'
)
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


def admin_handler(event, context):
    # SQL query to run
    cur.execute("SQL STATEMENT")
    output = cur.fetchall()
    # Changes the result to an int
    sql_result = []
    for i, in output:
        sql_result.append(i)
    sql_result_two = int(sql_result[0])
    
    # Flow control for SQL result
    if sql_result_two > 2:
        cur.execute(
            "SQL STATEMENT")
        output2 = cur.fetchall()

        # Converts nested tuples to lists and formats all elements to strings
        output3 = (list(output2))
        nested_output = [list(ele) for ele in output3]
        formatted_output = [map(str, x) for x in nested_output]

        # Sends the email
        SENDER = "Sender Name <no-reply@sendername.com>"
        recipients = ["name@place.com", "nametwo@place.com", "namethree@place.com"]

        RECIPIENT = ", ".join(recipients)

        # SMTP
        USERNAME_SMTP = secret_ses['example']
        PASSWORD_SMTP = secret_ses['example']
        HOST = "host"
        PORT = 465
        AWS_REGION = "region"
        SUBJECT = "Email Subject"
        BODY_TEXT = (f"html error, return {sql_result_two}")
        BODY_HTML = f""" <html>
                          <head>
                          </head>
                          <body>
                          <html><body style="font: 16px/1 Georgia, Serif;">
                          <h3>return {sql_result_two}</h3>
                          <table rules="all" border="1" style="border-color: #666;" cellpadding="10"><thead><tr style="background: #eee;"><th>one</th><th>two</th><th>three</th></tr></thead><tbody>"""
        for row in formatted_output:
            BODY_HTML = BODY_HTML + "<tr>"
            for col in row:
                BODY_HTML = BODY_HTML + "<td>" + col.replace(" ", "") + "</td>"
            BODY_HTML = BODY_HTML + "</tr>"
        BODY_HTML = BODY_HTML + "</table>"
        CHARSET = "UTF-8"

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
                server.login(var, var2)
                server.sendmail(SENDER, RECIPIENT, msg.as_string())
                server.close()


        except SMTPException as e:
            print("Error: ", e)

    else:
        print("No Alert Needed")

    return "Success"








