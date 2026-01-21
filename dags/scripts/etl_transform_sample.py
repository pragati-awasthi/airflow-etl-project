import pandas as pd

def transform_data(**kwargs):
    # Pull data from previous task
    extracted_data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract_task')
    df = pd.DataFrame(extracted_data)
    
    # Example transform: add full_name column
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    
    # Push transformed data to XCom
    kwargs['ti'].xcom_push(key='transformed_data', value=df.to_dict(orient='records'))
    print("Transformation complete.")
