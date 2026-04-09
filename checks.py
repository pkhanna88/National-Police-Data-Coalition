import pandas as pd
import os

# Load tables
base_dir = os.path.join(os.path.dirname(__file__), 'dashboard_data')
files = {
    'complaints.csv': os.path.join(base_dir, 'complaints.csv'),
    'allegations.csv': os.path.join(base_dir, 'allegations.csv'),
    'agencies.csv': os.path.join(base_dir, 'agencies.csv'),
    'officer_penalties.csv': os.path.join(base_dir, 'officer_penalties.csv'),
    'annual_trends.csv': os.path.join(base_dir, 'annual_trends.csv'),
    'agency_summary.csv': os.path.join(base_dir, 'agency_summary.csv')
}

dataframes = {}
for name, path in files.items():
    df = pd.read_csv(path)
    dataframes[name] = df
    print(f'{name}: {df.shape[0]} rows x {df.shape[1]} columns')
    for col in df.columns:
        nulls  = df[col].isna().sum()
        unique = df[col].nunique()
        sample = df[col].dropna().iloc[0] if df[col].notna().any() else 'All Null'
        print(f'  {col}: null_values={nulls}, unique_values={unique}, sample_value={repr(sample)}')
    print()

# Read tables
df_complaints = dataframes['complaints.csv']
df_allegations = dataframes['allegations.csv']
df_agencies = dataframes['agencies.csv']
df_penalties = dataframes['officer_penalties.csv']
df_trends = dataframes['annual_trends.csv']
df_summary = dataframes['agency_summary.csv']

# Validation checks
checks = {
    "complaints.csv: no duplicate complaint_id values":
        df_complaints['complaint_id'].nunique() == len(df_complaints),
    "complaints.csv: no negative days_to_close values":
        (df_complaints['days_to_close'].dropna() >= 0).all(),
    "allegations.csv: no .0 in officer_id values":
        not df_allegations[df_allegations['officer_id'] != 'Unidentified']['officer_id'].str.contains(r'\.').any(),
    "allegations.csv: all complaint_id values exist within complaints":
        df_allegations['complaint_id'].isin(df_complaints['complaint_id']).all(),
    "allegations.csv: no null findings":
        df_allegations['finding'].isna().sum() == 0,
    "penalties.csv: no duplicate officer_id values":
        df_penalties['officer_id'].nunique() == len(df_penalties),
    "agencies.csv: no duplicate agency_id values":
        df_agencies['agency_id'].nunique() == len(df_agencies),
    "annual_trends.csv: no null values":
        df_trends.isna().sum().sum() == 0,
    "agency_summary.csv: all agency_id value exists within agencies":
        df_summary['agency_id'].isin(df_agencies['agency_id']).all()
}

# Validation check results
all_pass = True
for check, result in checks.items():
    status = "True" if result else "False"
    if not result:
        all_pass = False
    print(f'{status} -- {check}')