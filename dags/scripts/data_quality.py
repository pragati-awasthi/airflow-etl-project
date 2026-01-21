import pandas as pd

def check_data_quality():
    df = pd.read_csv("/opt/airflow/data/Amazon.csv")

    print("Total Rows:", len(df))

    # Missing values
    missing = df.isnull().sum().sum()
    completeness = 100 - (missing / df.size * 100)

    # Duplicate orders
    duplicate_orders = df["OrderID"].duplicated().sum()

    # Invalid quantity or price
    invalid_quantity = df[df["Quantity"] <= 0].shape[0]
    invalid_price = df[df["UnitPrice"] <= 0].shape[0]

    print("Completeness %:", round(completeness, 2))
    print("Duplicate OrderIDs:", duplicate_orders)
    print("Invalid Quantity Rows:", invalid_quantity)
    print("Invalid UnitPrice Rows:", invalid_price)
