import requests
from google.cloud import bigquery
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import json

bq_client = bigquery.Client()

endpoint = "http://localhost:8000/api/v1/jobs/get_last_replication_job"
logs = "http://localhost:8000/api/v1/jobs/get"

username = "airbyte"
password = "password"

connection_id = "CONNECTION ID"  # we can find the connection id from the airbyte url

data = {"connectionId": connection_id}

headers = {"Accept": "application/json", "Content-Type": "application/json"}

response = requests.post(
    endpoint, headers=headers, json=data, auth=(username, password)
)

dicts = response.json()

if dicts["job"]["status"] == "succeeded":
    query = """CALL `mssql_ds_dbo.mssql_dimaccount`()"""
    bq_client.query(query).result()
else:
    print("JOBS GOT FAILED")
    job_id = dicts["job"]["id"]
    job_data = {"id": job_id}
    log_res = requests.post(
        logs, headers=headers, json=job_data, auth=(username, password)
    )
    logs = log_res.json()
    for log in logs["attempts"]:
        for fail in log["attempt"]["failureSummary"]["failures"]:
            print(fail["externalMessage"])
            failed_message = fail["externalMessage"]
            break
    # email process starting point
    current_date = datetime.now().strftime("%Y-%m-%d")
    sender_email = "praveenraaj.ks@thecloudside.com"
    receiver_email = ["praveenraaj0200@gmail.com"]

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_email)
    message[
        "Subject"
    ] = "Alerting Mail for failed Airbyte connection related to Stored Procedure - {}".format(
        current_date
    )
    smtp_password = "iwnbssbniwhrxwzt"

    html_body = f"""
        <html> 
    	    <body> 
    	        <p>Stored procedure -- Inventory_daily_stock_info is not triggered as the linked Airbyte connection got failed.</p></br> 
    	        <br> 
    	        <p> Failed Message -- {failed_message} </p> 
    	   </body> 
    	 </html> """

    # attach the message to the MIME message object
    message.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.starttls()
        smtp.login(sender_email, smtp_password)
        smtp.send_message(message)

