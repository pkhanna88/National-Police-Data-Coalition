from neo4j import GraphDatabase
from dotenv import load_dotenv
import pandas as pd
import os

# Connect to neo4j database via driver
load_dotenv()
uri = os.getenv("neo4j_uri")
username = os.getenv("neo4j_username")
password = os.getenv("neo4j_password")
driver = GraphDatabase.driver(uri, auth = (username, password))

# Create output folder
os.makedirs("dashboard_data", exist_ok=True)

# Runs the cypher query and returns the result as a pandas dataframe
def run_query(cypher):
    with driver.session() as session:
        result = session.run(cypher)
        df = pd.DataFrame([r.data() for r in result])
    return df


# Query 1: Complaints
complaints_query = """
MATCH (c:Complaint)
OPTIONAL MATCH (c)-[:OCCURRED_AT]->(loc:Location)
OPTIONAL MATCH (loc)-[:WITHIN_CITY]->(city:CityNode)
OPTIONAL MATCH (city)-[:WITHIN_STATE]->(state:StateNode)
RETURN
    id(c)                    AS complaint_id,
    c.record_id              AS record_id,
    c.incident_date          AS incident_date,
    c.closed_date            AS closed_date,
    c.reason_for_contact     AS reason_for_contact,
    c.outcome_of_contact     AS outcome_of_contact
"""

# Run query
df_complaints = run_query(complaints_query)

# Convert the date strings to datetime so that calculations can be done upon them
df_complaints['incident_date'] = pd.to_datetime(df_complaints['incident_date'], errors = 'coerce')
df_complaints['closed_date'] = pd.to_datetime(df_complaints['closed_date'], errors = 'coerce')

# Add days to close column which calculates how many days passed between the incident and case closing
df_complaints['days_to_close'] = (df_complaints['closed_date'] - df_complaints['incident_date']).dt.days

# Add incident month and year columns with the extracted month and year values from the date
df_complaints['incident_month'] = df_complaints['incident_date'].dt.month
df_complaints['incident_month_name'] = df_complaints['incident_date'].dt.strftime('%B')
df_complaints['incident_year'] = df_complaints['incident_date'].dt.year
df_complaints['incident_year'] = pd.to_numeric(df_complaints['incident_year'],  errors = 'coerce').astype('Int64')
df_complaints['incident_month'] = pd.to_numeric(df_complaints['incident_month'], errors = 'coerce').astype('Int64')

# Add a month sort column to allow for better calendar sorting
month_order = {
    1:'01-Jan', 2:'02-Feb', 3:'03-Mar', 4:'04-Apr', 
    5:'05-May', 6:'06-Jun', 7:'07-Jul', 8:'08-Aug', 
    9:'09-Sep', 10:'10-Oct', 11:'11-Nov', 12:'12-Dec'}
df_complaints['month_sort'] = df_complaints['incident_month'].map(month_order)

# Save table
df_complaints.to_csv("dashboard_data/complaints.csv", index = False)
print(f"complaints.csv: {df_complaints.shape[0]} rows x {df_complaints.shape[1]} columns")


# Query 2: Allegations
allegations_query = """
MATCH (c:Complaint)-[:ALLEGED]->(a:Allegation)
OPTIONAL MATCH (o:Officer)-[:ACCUSED_OF]->(a)
OPTIONAL MATCH (a)-[:RESULTS_IN]->(p)
RETURN
    id(a)                     AS allegation_id,
    id(c)                     AS complaint_id,
    id(o)                     AS officer_id,
    a.allegation              AS allegation_type,
    a.finding                 AS finding,
    a.outcome                 AS outcome
"""

# Run query
df_allegations = run_query(allegations_query)

# Fill in the blanks for null officer_id values with Unidentified
df_allegations['officer_id'] = df_allegations['officer_id'].fillna('Unidentified')

# Convert officer_id from float to integer to string to ensure there are no decimal values
def clean_officer_id(val):
    if val == 'Unidentified':
        return 'Unidentified'
    try:
        return str(int(float(val)))
    except ValueError:
        return 'Unidentified'
df_allegations['officer_id'] = df_allegations['officer_id'].apply(clean_officer_id)

# Save table
df_allegations.to_csv("dashboard_data/allegations.csv", index = False)
print(f"allegations.csv: {df_allegations.shape[0]} rows x {df_allegations.shape[1]} columns")


# Query 3: Agencies
agencies_query = """
MATCH (a:Agency)
OPTIONAL MATCH (a)-[:WITHIN_STATE]->(state:StateNode)
OPTIONAL MATCH (a)-[:WITHIN_CITY]->(city:CityNode)
RETURN
    id(a)            AS agency_id,
    a.name           AS agency_name,
    a.jurisdiction   AS jurisdiction,
    a.hq_address     AS hq_address,
    a.hq_city        AS hq_city,
    a.hq_state       AS hq_state,
    a.website_url    AS website_url,
    state.name       AS state_name,
    city.name        AS city_name
"""

# Run query
df_agencies = run_query(agencies_query)

# Add a final city and state column which uses either the hq values or city/state depending on which is available
df_agencies['final_city'] = df_agencies['hq_city'].combine_first(df_agencies['city_name'])
df_agencies['final_state'] = df_agencies['hq_state'].combine_first(df_agencies['state_name'])

# Drop the source columns since they are now redundant
df_agencies = df_agencies.drop(columns = ['hq_city', 'city_name', 'state_name'])

# Save table
df_agencies.to_csv("dashboard_data/agencies.csv", index = False)
print(f"agencies.csv: {df_agencies.shape[0]} rows x {df_agencies.shape[1]} columns")


# Query 4: Penalties
penalties_query = """
MATCH (o:Officer)-[:RECEIVED]->(p:Penalty)
RETURN
    id(o)            AS officer_id,
    o.first_name     AS first_name,
    o.last_name      AS last_name,
    p.penalty        AS penalty
"""

# Run query
df_penalties = run_query(penalties_query)

# Add penalty category column which assigns the penalty to a specific category for better sorting 
def categorize_penalty(p):
    if p == 'No penalty':
        return 'No penalty issued'
    elif 'Decision Pending' in str(p):
        return 'Decision pending'
    elif p == 'Retired':
        return 'Retired before decision'
    elif p == 'Closed Administratively (Instructions)':
        return 'Administrative closure'
    else:
        return 'Disciplinary action'
df_penalties['penalty_category'] = df_penalties['penalty'].apply(categorize_penalty)

# Penalty priority from most to least significant for officers who received multiple penalties
priority = {
    'Disciplinary action':      1,
    'Administrative closure':   2,
    'Decision pending':         3,
    'Retired before decision':  4,
    'No penalty issued':        5
}
df_penalties['priority'] = df_penalties['penalty_category'].map(priority)
df_penalties = df_penalties.sort_values('priority')
df_penalties = df_penalties.drop_duplicates(subset=['officer_id'], keep = 'first')
df_penalties = df_penalties.drop(columns=['priority'])
df_penalties = df_penalties.sort_values(['last_name', 'first_name']).reset_index(drop = True)

# Save table
df_penalties.to_csv("dashboard_data/officer_penalties.csv", index = False)
print(f"officer_penalties.csv: {df_penalties.shape[0]} rows x {df_penalties.shape[1]} columns")


# Query 5: Annual Trends
trends_query = """
MATCH (c:Complaint)
WHERE c.incident_date IS NOT NULL
RETURN
    substring(toString(c.incident_date), 0, 4) AS year,
    c.reason_for_contact                       AS reason_for_contact,
    count(c)                                   AS complaint_count
ORDER BY year
"""

# Run query
df_trends = run_query(trends_query)

# Fill in the blanks for null reason_for_contact values with Not recorded
df_trends['reason_for_contact'] = df_trends['reason_for_contact'].replace({'': 'Not recorded'}).fillna('Not recorded')

# Save table
df_trends.to_csv("dashboard_data/annual_trends.csv", index = False)
print(f"annual_trends.csv: {df_trends.shape[0]} rows x {df_trends.shape[1]} columns")


# Query 6: Agency Summary
agency_summary_query = """
MATCH (a:Agency)-[:ESTABLISHED]->(u:Unit)
MATCH (o:Officer)-[:MEMBER_OF_UNIT]->(u)
MATCH (o)-[:ACCUSED_OF]->(al:Allegation)
MATCH (c:Complaint)-[:ALLEGED]->(al)
RETURN
    id(a)                     AS agency_id,
    a.name                    AS agency_name,
    a.hq_state                AS state,
    count(DISTINCT c)         AS total_complaints,
    count(DISTINCT o)         AS officers_with_complaints,
    count(al)                 AS total_allegations
ORDER BY total_complaints DESC
"""

# Run query
df_agency_summary = run_query(agency_summary_query)

# Save table
df_agency_summary.to_csv("dashboard_data/agency_summary.csv", index = False)
print(f"agency_summary.csv: {df_agency_summary.shape[0]} rows x {df_agency_summary.shape[1]} columns")


# Close driver connection
driver.close()
