from google.cloud import bigquery
bq_client = bigquery.Client()  # clinet Initialization

if dicts['job']['status'] == 'succeeded':
    query = '''CALL `mssql_ds_dbo.mssql_dimaccount`()'''
    bq_client.query(query).result()  # Stored Procedure Call
else:
    print 'JOBS GOT FAILED'

    # Initiating Email alerting process

