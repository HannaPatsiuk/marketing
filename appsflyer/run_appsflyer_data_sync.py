import io
import os
import datetime
import locale
import logging
import time
import requests
from google.cloud import bigquery
import pandas as pd

logger = logging.getLogger()

def run_appsflyer_data_sync(event, context):
    APPSFLYER_API_URL = os.environ["APPSFLYER_API_URL"]
    APPSFLYER_API_TOKEN = os.environ["APPSFLYER_API_TOKEN"]

    if "is_reattr" in event:
        is_reattr = event["is_reattr"]
    else:
        is_reattr = False
    if "bigquery_dataset" in event:
        target_dataset = event["bigquery_dataset"]
    else:
        target_dataset = "play_tables"
    if "bigquery_table" in event:
        target_table = event["bigquery_table"]
    else:
        target_table = "AppsFlyer_DS"

    targetDate = datetime.datetime.now().date() - datetime.timedelta(1)
    targetDateStr = targetDate.strftime("%Y-%m-%d")
    startDateStr = "2020-01-01"
    data = fetch_appsflyer_data(APPSFLYER_API_URL, APPSFLYER_API_TOKEN, startDateStr, targetDateStr, is_reattr)
    if not data is None:
        import_to_big_query(target_dataset, target_table, data, is_reattr)

def fetch_appsflyer_data(url, token, startDate, endDate, reattr):
    baseUrl = url
    appsFlyerRequestUrl = baseUrl + "api_token=" + token + "&from=" + startDate + "&to=" + endDate + "&timezone=America/Panama"
    if reattr:
        appsFlyerRequestUrl += "&reattr=true"
    logger.info(appsFlyerRequestUrl)
    rawResponse = requests.get(appsFlyerRequestUrl)
    if rawResponse.status_code == 200:
        data = io.StringIO(rawResponse.text)
        csv = pd.read_csv(data)
        idx = pd.IndexSlice
        csv["decision (Event counter)"] = 0
        csv["decision (Unique users)"] = 0
        csv["loan decision delivered (Unique users)"] = 0
        csv["loan decision delivered (Event counter)"] = 0           
        if reattr:
            csv["Installs"] = 0
            csv["source"] = "reattr"
        else:
            csv["source"] = "attr"

        csv.update(csv.loc[idx[:, "Total Cost"]].fillna(value=0))

        csv_filtered = csv[[
                "Date",
                "Agency/PMD (af_prt)",	
                "Media Source (pid)",
                "Campaign (c)",
                "Installs",
                "Total Cost",
                "approve (Unique users)",
                "approve (Event counter)",
                "decision (Unique users)",
                "decision (Event counter)",
                "loandecisiondelivered (Unique users)",         
                "loandecisiondelivered (Event counter)",
                "loan decision delivered (Unique users)",         
                "loan decision delivered (Event counter)",
                "signup (Unique users)",
                "signup (Event counter)",
                "source"]]

        normalized = csv_filtered.rename(columns={"Date": "Date","Agency/PMD (af_prt)": "Agency_PMD__af_prt_",
        "Media Source (pid)": "Media_Source__pid_", "Campaign (c)": "Campaign__c_", "Installs": "Installs", "Total Cost": "Total_Cost",
        "approve (Unique users)": "approve__Unique_users_","approve (Event counter)":"approve__Event_counter_",
        "decision (Unique users)": "decision__Unique_users_", "decision (Event counter)": "decision__Event_counter_",
        "loandecisiondelivered (Unique users)": "loandecisiondelivered__Unique_users_",         
        "loandecisiondelivered (Event counter)": "loandecisiondelivered__Event_counter_",
        "loan decision delivered (Unique users)": "loan_decision_delivered__Unique_users_",         
        "loan decision delivered (Event counter)": "loan_decision_delivered__Event_counter_",
        "signup (Unique users)": "signup__Unique_users_","signup (Event counter)":"signup__Event_counter_",
        "source": "source"})
        return normalized
    else:
        logger.error("Error calling Appsflyer API with URL: " + appsFlyerRequestUrl)
        return None

def import_to_big_query(dataset, table, csv, is_reattr):
    job_config = bigquery.LoadJobConfig()
    job_config.skip_leading_rows = 1
    job_config.ignore_unknown_values = True
    job_config.schema = [
        bigquery.SchemaField("Date", "DATE"),
        bigquery.SchemaField("Agency_PMD__af_prt_", "STRING"),
        bigquery.SchemaField("Media_Source__pid_", "STRING"),
        bigquery.SchemaField("Campaign__c_", "STRING"),
        bigquery.SchemaField("Installs", "INTEGER"),
        bigquery.SchemaField("Total_Cost", "FLOAT"),
        bigquery.SchemaField("approve__Unique_users_", "INTEGER"),
        bigquery.SchemaField("approve__Event_counter_", "INTEGER"),
        bigquery.SchemaField("decision__Unique_users_", "INTEGER"),
        bigquery.SchemaField("decision__Event_counter_", "INTEGER"),
        bigquery.SchemaField("loandecisiondelivered__Unique_users_", "INTEGER"),
        bigquery.SchemaField("loandecisiondelivered__Event_counter_", "INTEGER"),
        bigquery.SchemaField("loan_decision_delivered__Unique_users_", "INTEGER"),
        bigquery.SchemaField("loan_decision_delivered__Event_counter_", "INTEGER"),
        bigquery.SchemaField("signup__Unique_users_", "INTEGER"),
        bigquery.SchemaField("signup__Event_counter_", "INTEGER"),
        bigquery.SchemaField("source", "STRING")]
        
    client = bigquery.Client(default_query_job_config=job_config)
    dataset_ref = client.dataset(dataset)
    table_ref = dataset_ref.table(table)
    
    if is_reattr:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    else:
        client.delete_table(table_ref)
    job = client.load_table_from_dataframe(csv, table_ref, job_config)
    job.result()
