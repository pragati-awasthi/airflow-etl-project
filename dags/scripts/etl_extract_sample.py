import pandas as pd

def extract_data(**kwargs):
    # Create dummy data
    data = {
        'id': [1, 2, 3, 4, 5],
        'first_name': ['John', 'Jane', 'Alice', 'Bob', 'Carol'],
        'last_name': ['Doe', 'Smith', 'Johnson', 'Brown', 'Davis'],
        'age': [28, 34, 23, 45, 31]
    }
    df = pd.DataFrame(data)
    # Push to XCom for next task
    kwargs['ti'].xcom_push(key='extracted_data', value=df.to_dict(orient='records'))
    print("Extraction complete.")
