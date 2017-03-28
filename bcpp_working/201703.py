import hashlib
import numpy as np
import pandas as pd

from datetime import datetime


surveys = {
    'YR1': 'bcpp-year-1',
    'YR2': 'bcpp-year-2',
    'YR3': 'bcpp-year-3',
}


def convert_date(value):
    if pd.isnull(value):
        return np.NaN
    else:
        return datetime.strptime(value, '%d%b%Y')


def identity256(value):
    identity256 = np.nan
    if pd.notnull(value):
        identity256 = hashlib.sha256(str(value).encode()).hexdigest()
    return identity256


df = pd.read_csv(
    '/Users/erikvw/Downloads/year2CPC_ALL_28MAR17.csv', low_memory=False)

df['final_hiv_status_date'] = df.apply(
    lambda row: convert_date(row['final_hiv_status_date']), axis=1)
df['interview_date'] = df.apply(
    lambda row: convert_date(row['interview_date']), axis=1)
df['cd4_date'] = df.apply(
    lambda row: convert_date(row['cd4_date']), axis=1)
df['prev_result_date'] = df.apply(
    lambda row: convert_date(row['prev_result_date']), axis=1)

df['survey'] = df['timepoint'].map(surveys.get)

df1 = pd.read_csv(
    '/Users/erikvw/bcpp_201703/bcpp_subject/subjectconsent.csv', low_memory=False, sep='|')
df_new = pd.merge(
    df, df1[['subject_identifier', 'identity']], on='subject_identifier', how='left')
df_new[pd.isnull(df_new['identity'])]
df_new = df_new.rename(columns={
    'identity': 'identity256',
    'cd4_test': 'cd4_tested'})
df_new['identity256'] = df_new.apply(
    lambda row: identity256(row['identity256']), axis=1)
df_new = df_new.rename(columns={'Pair': 'pair'})

columns = [
    'age_at_interview', 'arv_clinic', 'cd4_date', 'cd4_tested', 'cd4_value',
    'circumcised', 'community', 'interview_date', 'final_arv_status', 'final_hiv_status',
    'final_hiv_status_date', 'gender', 'identity256', 'marital_status', 'pregnant',
    'prev_result_known', 'prev_result',
    'prev_result_date', 'referred', 'self_reported_result', 'subject_identifier',
    'survey', 'education', 'working', 'job_type', 'number_partners_last12months',
    'pair', 'referral_code', 'timepoint'
]
columns.sort()

options = dict(
    path_or_buf='/Users/erikvw/bcpp_201703/cdc_year2_cpc_20170328.csv',
    na_rep='',
    encoding='utf8',
    date_format='%Y-%m-%d %H:%M:%S',
    index=False,
    columns=columns)
df_new.to_csv(**options)
options = dict(
    path_or_buf='/Users/erikvw/bcpp_201703/cdc_year2_cpc_20170328_pipe.csv',
    na_rep='',
    encoding='utf8',
    date_format='%Y-%m-%d %H:%M:%S',
    index=False,
    sep='|',
    columns=columns)
df_new.to_csv(**options)
