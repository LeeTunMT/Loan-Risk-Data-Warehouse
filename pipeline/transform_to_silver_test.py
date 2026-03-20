"""
This module aim to: handle null data, type casting, remove outliers,
deduplicate, join tables (train and test of application), nomarlize data.
The transformed data will be stored in silver stage in BigQuery.
"""
from datetime import datetime
from google.cloud import bigquery
import pandas as pd 
import numpy as np

# this function is for data quality check, its rules defined by me (Bussiness understanding and data understanding phase)
# check null values, data types, duplicates, outliers (if necessary) 
def data_quqality_check(df):
    return 

def transform_to_silver():
    project_id = 'loan-risk-home-credit'
    dataset_bronze = 'bronze_stage'
    dataset_silver = 'silver_stage'
    client = bigquery.Client(project=project_id)

    TABLES_TO_TRANSFORM = [
        'application_test',
        'application_train',
        'bureau', 
        'bureau_balance', 
        'credit_card_balance', 
        'installments_payments', 
        'previous_application', 
        'POS_CASH_balance'
    ]
    # application_test and application_train
    df1 = pd.read_gbq(f"""
        SELECT *
        FROM `{project_id}.{dataset_bronze}.application_train`
    """, project_id=project_id)
    df2 = pd.read_gbq(f"""
        SELECT *
        FROM `{project_id}.{dataset_bronze}.application_test`
    """, project_id=project_id)
    # lower case column names
    df1.columns = df1.columns.str.lower()
    df2.columns = df2.columns.str.lower()

    # Concatenate train and test dataframes, replace index 
    # remove column target and get a new dataframe with only sk_id_curr and target -> new table
    df_target = df1[['sk_id_curr', 'target']]
    df1.drop(columns='target', inplace=True)

    df = pd.concat([df1, df2], ignore_index=True)
    del df1, df2

    # remove duplicates based on sk_id_curr
    df.drop_duplicates(subset='sk_id_curr', inplace=True)

    # round float64 columns to 4 decimal places to reduce storage size
    df[df.select_dtypes(include=['float64']).columns] = df[df.select_dtypes(include=['float64']).columns].round(4)

    cols_days = df.filter(like='days').columns

    df.loc[df[cols_days] > 36500, cols_days] = np.nan

    df['age'] = (abs(df['days_birth'])/365).astype('int64')
    df['date_birth'] = pd.to_datetime('today') + pd.to_timedelta(df['days_birth'], unit='D')
    df['date_birth'] = df['date_birth'].dt.strftime('%Y-%m-%d')
    
    df['date_employed'] = pd.to_datetime('today') + pd.to_timedelta(df['days_employed'], unit='D')
    df['date_employed'] = df['date_employed'].dt.strftime('%Y-%m-%d')

    df['date_registration'] = pd.to_datetime('today') + pd.to_timedelta(df['days_registration'], unit='D')
    df['date_registration'] = df['date_registration'].dt.strftime('%Y-%m-%d')

    df['date_last_phone_change'] = pd.to_datetime('today') + pd.to_timedelta(df['days_last_phone_change'], unit='D')
    df['date_last_phone_change'] = df['date_last_phone_change'].dt.strftime('%Y-%m-%d')

    df['date_id_publish'] = pd.to_datetime('today') + pd.to_timedelta(df['days_id_publish'], unit='D')
    df['date_id_publish'] = df['date_id_publish'].dt.strftime('%Y-%m-%d')

    df['annuity_to_credit_ratio'] = df['amt_annuity'] / df['amt_credit']
    df['credit_to_income_ratio'] = df['amt_credit'] / df['amt_income_total']
    df['annuity_to_income_ratio'] =  df['amt_annuity'] / df['amt_income_total']
    df['credit_term'] = df['amt_credit'] / df['amt_annuity'].replace(0, np.nan)



    df.to_gbq(f"{dataset_silver}.application", project_id=project_id, if_exists='replace')
    df_target.to_gbq(f"{dataset_silver}.application_target", project_id=project_id, if_exists='replace')
    del df, df_target

    # bureau
    df = pd.read_gbq(f"""
        select * from `{project_id}.{dataset_bronze}.bureau`
    """, project_id=project_id)
    df.columns = df.columns.str.lower()
    df[df.select_dtypes(include=['float64']).columns] = df[df.select_dtypes(include=['float64']).columns].round(4)
    df['last_credit_date'] = pd.to_datetime('today') + pd.to_timedelta(df['days_credit'], unit='D')
    df['date_credit_enddate'] = pd.to_datetime('today') + pd.to_timedelta(df['days_credit_enddate'], unit='D')
    # adjust days_credit_update to be the negative value
    df.loc[df['days_credit_update'] > 0, 'days_credit_update'] *= -1
    df['date_credit_update'] = pd.to_datetime('today') + pd.to_timedelta(df['days_credit_update'], unit='D')

    df['annuity_to_credit_ratio'] = df['amt_annuity'] / df['amt_credit_sum']
    df.to_gbq(f"{dataset_silver}.bureau", project_id=project_id, if_exists='replace')
    del df

    # bureau_balance
    df = pd.read_gbq(f"""
        select * from `{project_id}.{dataset_bronze}.bureau_balance`
    """, project_id=project_id)
    df.columns = df.columns.str.lower()
    df[df.select_dtypes(include=['float64']).columns] = df[df.select_dtypes(include=['float64']).columns].round(4)
    status_type = {
        "C" : "Closed",
        "X" : "Unknown",
        "0" : "No DPD",
        "1" : "DPD 1-30 days",
        "2" : "DPD 31-60 days",
        "3" : "DPD 61-90 days",
        "4" : "DPD 91-120 days",
        "5" : "DPD > 120 days"
    }
    df['status'] = df['status'].map(status_type)

    df.to_gbq(f"{dataset_silver}.bureau_balance", project_id=project_id, if_exists='replace')
    del df

    # credit_card_balance
    df = pd.read_gbq(f"""
        select * from `{project_id}.{dataset_bronze}.credit_card_balance`
    """, project_id=project_id)
    df.columns = df.columns.str.lower()
    df[df.select_dtypes(include=['float64']).columns] = df[df.select_dtypes(include=['float64']).columns].round(4)
    df['year_month_balance'] = pd.to_datetime('today') + pd.to_timedelta(df['months_balance'] * 30, unit='D')
    df['year_month_balance'] = df['year_month_balance'].dt.strftime('%Y-%m')

    df.to_gbq(f"{dataset_silver}.credit_card_balance", project_id=project_id, if_exists='replace')
    del df

    # POS_CASH_balance
    df = pd.read_gbq(f"""
        select * from `{project_id}.{dataset_bronze}.pos_cash_balance`
    """, project_id=project_id)
    df.columns = df.columns.str.lower()
    df[df.select_dtypes(include=['float64']).columns] = df[df.select_dtypes(include=['float64']).columns].astype('Int64')
    df['year_month_balance'] = pd.to_datetime('today') + pd.to_timedelta(df['months_balance'] * 30, unit='D')
    df['year_month_balance'] = df['year_month_balance'].dt.strftime('%Y-%m')

    df.to_gbq(f"{dataset_silver}.pos_cash_balance", project_id=project_id, if_exists='replace')
    del df

    # installments_payments
    df = pd.read_gbq(f"""
        select * from `{project_id}.{dataset_bronze}.installments_payments`
    """, project_id=project_id)
    df.columns = df.columns.str.lower()
    df['date_installment'] = pd.to_datetime('today') + pd.to_timedelta(df['days_instalment'], unit='D')
    df['date_installment'] = df['date_installment'].dt.strftime('%Y-%m-%d')
    df['date_entry_payment'] = pd.to_datetime('today') + pd.to_timedelta(df['days_entry_payment'], unit='D')
    df['date_entry_payment'] = df['date_entry_payment'].dt.strftime('%Y-%m-%d')
    # check if amt_instalment is zero to avoid division by zero
    df['ratio_payment_installment'] = df['amt_payment'] / df['amt_instalment'].replace(0, np.nan).round(4)
    df['payment_delay'] = df['amt_instalment'] - df['amt_payment']
    df.to_gbq(f"{dataset_silver}.installments_payments", project_id=project_id, if_exists='replace')
    del df
    # previous_application
    df = pd.read_gbq(f"""
        select * from `{project_id}.{dataset_bronze}.previous_application`
    """, project_id=project_id)
    df.columns = df.columns.str.lower()
    #  days_first_due days_last_due_1st_version  days_last_due days_termination
    df['date_decision'] = pd.to_datetime('today') + pd.to_timedelta(df['days_decision'], unit='D')
    df['date_decision'] = df['date_decision'].dt.strftime('%Y-%m-%d')

    df['date_first_due'] = pd.to_datetime('today') + pd.to_timedelta(df['days_first_due'], unit='D')
    df['date_first_due'] = df['date_first_due'].dt.strftime('%Y-%m-%d')
    df['date_last_due_1st_version'] = pd.to_datetime('today') + pd.to_timedelta(df['days_last_due_1st_version'], unit='D')
    df['date_last_due_1st_version'] = df['date_last_due_1st_version'].dt.strftime('%Y-%m-%d')   
    df['date_last_due'] = pd.to_datetime('today') + pd.to_timedelta(df['days_last_due'], unit='D')
    df['date_last_due'] = df['date_last_due'].dt.strftime('%Y-%m-%d')   
    df['date_termination'] = pd.to_datetime('today') + pd.to_timedelta(df['days_termination'], unit='D')
    df['date_termination'] = df['date_termination'].dt.strftime('%Y-%m-%d')

    df['nflag_insured_on_approval'] = df['nflag_insured_on_approval'].astype('Int64')
    dict_flag = {
        0: 'Not Insured',
        1: 'Insured'
    }
    df['nflag_insured_on_approval'] = df['nflag_insured_on_approval'].map(dict_flag)

    df.to_gbq(f"{dataset_silver}.previous_application", project_id=project_id, if_exists='replace')
    del df

if __name__ == "__main__":
    transform_to_silver()
