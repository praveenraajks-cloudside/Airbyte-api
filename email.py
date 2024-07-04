from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

logs = "http://localhost:8000/api/v1/jobs/get"  # for getting Logs

job_id = dicts["job"]["id"]  # job id from raw output of #2 step


job_data = {"id": job_id}


log_response = requests.post(
    logs, headers=headers, json=job_data, auth=(username, password)
)


if dicts["job"]["status"] == "succeeded":
    print("test")
    # INVOKING STORED PROCEDURE
else:
    print("JOBS GOT FAILED")
    # Explained this steps in the Note scetion below
    logs = log_response.json()
    for log in logs["attempts"]:
        for fail in log["attempt"]["failureSummary"]["failures"]:
            print(fail["externalMessage"])
            failed_message = fail["externalMessage"]
            break

    # Email templating stpes
    current_date = datetime.now().strftime("%Y-%m-%d")
    sender_email = "SENDER EMAIL"
    receiver_email = ["RECEIVER EMAIL"]

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_email)
    message[
        "Subject"
    ] = "Alerting Mail for failed Airbyte connection related to Stored Procedure - {}".format(
        current_date
    )
    smtp_password = (
        "APP_PASSWORD"  # we can generate the app_password by following the link
    )

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

