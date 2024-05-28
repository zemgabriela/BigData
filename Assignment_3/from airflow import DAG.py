from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import io

# Default arguments for the DAG
default_args = {
    'owner': 'Gabriela & Nacho', # Owner
    'start_date': datetime.now() - timedelta(days=1), # Start date 
    'depends_on_past': False, # Past runs dependancy
    'email': ['gzemenze7@alumnes.ub.edu'], # Email address 
    'email_on_failure': True, # Send email on failure
    'email_on_retry': False, # Do not send email on retry
    'retries': 1, # Number of retries
    'retry_delay': timedelta(minutes=1) # Time between retries
}

# ===== TASK ONE ====
# Download dataset and store locally.
def download_dataset():

    data_url = "https://archive.ics.uci.edu/static/public/352/data.csv"
    response = requests.get(data_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Convert the content of the response to a pandas DataFrame
        df = pd.read_csv(io.StringIO(response.text))
        print("Dataset downloaded and stored in a DataFrame successfully!")
    else:
        print(f"Failed to download the dataset. Status code: {response.status_code}")

    # Store it into the designated data directory
    df.to_csv("data/train.csv", index = False)
    
# ===== TASK TWO ====
# Clean data
def data_cleaning():
    # ===== HANDLE MISSING VALUES ====
    # Read csv file
    df = pd.read_csv("data/train.csv")
    # Fill missing values in Description based on StockCode
    stockcode_description_map = (df.dropna(subset=['Description'])
                                .drop_duplicates('StockCode')[['StockCode', 'Description']]
                                .set_index('StockCode')['Description']
                                .to_dict())
    df['Description'] = df['Description'].fillna(df['StockCode'].map(stockcode_description_map))
    print("Missing values in Description column filled successfully!")

    # ===== CONVERT DATA TYPES ====
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'], format='%m/%d/%Y %H:%M')
    df['CustomerID'] = df['CustomerID'].astype('float').astype('Int64').astype(str)
    
    # ===== REMOVE DUPLICATES VALUES ====
    df = df.drop_duplicates().reset_index(drop = True)
    
    # Save it
    df.to_csv("data/df_clean.csv", index = False)

# ===== TASK THREE ====
# Data transformation
def data_transformation():
    # Read csv file
    df = pd.read_csv("data/df_clean.csv")
    # Calculate
    df['total_price'] = df.Quantity * df.UnitPrice
    # Save it
    df.to_csv("data/df_clean.csv", index = False)

# ===== TASK FOUR ====
def mongodb_load():
    

# Define the DAG
dag = DAG(
    "Nacho_Gabriela_dag",
    description='A DAG scheduled for midnight every day',
    default_args=default_args,
    catchup=False,
    schedule_interval='0 0 * * *'  # Schedule at midnight every day
)
with dag:
    # Task 1
    t1 = PythonOperator(task_id="Task1",python_callable=download_dataset)

    # Task 2
    t2 = PythonOperator(task_id="Task2",python_callable=data_cleaning)

    # Task 3
    t3 = PythonOperator(task_id="Task3",python_callable=data_transformation)

    # Task 4
    t4 = PythonOperator(task_id="Task4",python_callable=mongodb_load)

# # Define dependencies
t1 >> t2 >> t3 >> t4


