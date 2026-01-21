import pandas as pd

def extract_data(ti):
    """
    Extracts data from CSV file and performs basic pre-processing.
    Pushes record count to XCom.
    """

    # CSV file path (change CSV here if needed)
    file_path = "/opt/airflow/data/Amazon.csv"

    # Read CSV
    df = pd.read_csv(file_path)

    # Basic pre-processing
    df.columns = df.columns.str.lower().str.strip()

    # Record count
    record_count = len(df)

    # Push data to XCom
    ti.xcom_push(key="record_count", value=record_count)

    print(f"Extracted {record_count} records from {file_path}")
