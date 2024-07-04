import requests
endpoint = 'http://localhost:8000/api/v1/jobs/get_last_replication_job'
username = 'airbyte'
password = 'password'
connection_id = 'CONNECTION_ID'
data = {'connectionId': connection_id}
headers = {'Accept': 'application/json',
           'Content-Type': 'application/json'}
response = requests.post(endpoint, headers=headers, json=data,
                         auth=(username, password))
dicts = response.json()

