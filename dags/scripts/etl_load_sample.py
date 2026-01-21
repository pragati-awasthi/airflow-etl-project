import pandas as pd

def load_data(**kwargs):
    # Pull transformed data
    transformed_data = kwargs['ti'].xcom_pull(key='transformed_data', task_ids='transform_task')
    df = pd.DataFrame(transformed_data)
    
    # For demo, just save to CSV (or could be DB)
    df.to_csv('dags/scripts/final_data.csv', index=False)
    print("Load complete. Data saved to final_data.csv")
