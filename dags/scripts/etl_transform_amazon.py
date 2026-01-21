# dags/scripts/etl_transform_sample.py

import pandas as pd
from datetime import datetime
import os

DATA_PATH = "/opt/airflow/data/Amazon.csv"


def load_amazon_data():
    """Load Amazon CSV data"""
    if not os.path.exists(DATA_PATH):
        raise FileNotFoundError("Amazon.csv not found in dags/data/")
    return pd.read_csv(DATA_PATH)


def transform_orders(**kwargs):
    """
    Transform Amazon order data:
    - Parse OrderDate
    - Calculate FinalAmount
    - Filter Delivered orders
    - Push stats to XCom
    """
    df = load_amazon_data()

    total_records = len(df)

    # Convert OrderDate to datetime
    df['OrderDate'] = pd.to_datetime(df['OrderDate'], errors='coerce')

    # Calculate Final Amount
    df['FinalAmount'] = (
        (df['Quantity'] * df['UnitPrice'])
        - (df['Quantity'] * df['UnitPrice'] * df['Discount'])
        + df['Tax']
        + df['ShippingCost']
    )

    # Filter delivered orders
    delivered_df = df[df['OrderStatus'] == 'Delivered']
    delivered_records = len(delivered_df)

    stats = {
        "total_records": total_records,
        "delivered_orders": delivered_records,
        "execution_date": kwargs['ds']
    }

    print("[Transform Orders] Stats:", stats)

    # Push stats to XCom
    kwargs['ti'].xcom_push(key="order_transform_stats", value=stats)


def transform_customer_metrics(**kwargs):
    """
    Customer-level metrics:
    - Total spend per customer
    - Number of orders
    """
    df = load_amazon_data()

    customer_metrics = (
        df.groupby('CustomerID')
        .agg(
            total_orders=('OrderID', 'count'),
            total_spent=('TotalAmount', 'sum')
        )
        .reset_index()
    )

    stats = {
        "unique_customers": customer_metrics.shape[0],
        "avg_orders_per_customer": round(customer_metrics['total_orders'].mean(), 2)
    }

    print("[Transform Customers] Stats:", stats)

    kwargs['ti'].xcom_push(key="customer_transform_stats", value=stats)


def partition_by_date(**kwargs):
    """
    Time-based partition logic using execution date
    """
    execution_date = kwargs['ds']
    print(f"[Partition] Processing Amazon data for date partition: {execution_date}")


def summarize_transform_stats(**kwargs):
    """
    Pull XCom stats and print summary
    """
    ti = kwargs['ti']
    order_stats = ti.xcom_pull(
        task_ids='transform_orders',
        key='order_transform_stats'
    )
    customer_stats = ti.xcom_pull(
        task_ids='transform_customer_metrics',
        key='customer_transform_stats'
    )

    summary = {
        "order_stats": order_stats,
        "customer_stats": customer_stats
    }

    print("[Transform Summary]", summary)

    ti.xcom_push(key="transform_summary", value=summary)
