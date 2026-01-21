import pandas as pd

def calculate_scoring_metrics():
    df = pd.read_csv("/opt/airflow/data/Amazon.csv")

    # Actual outcome
    df["actual"] = df["OrderStatus"].apply(
        lambda x: 1 if str(x).lower() in ["completed", "delivered"] else 0
    )

    # Prediction logic
    df["predicted"] = df["Discount"].apply(lambda x: 1 if x > 0 else 0)

    TP = ((df["actual"] == 1) & (df["predicted"] == 1)).sum()
    TN = ((df["actual"] == 0) & (df["predicted"] == 0)).sum()
    FP = ((df["actual"] == 0) & (df["predicted"] == 1)).sum()
    FN = ((df["actual"] == 1) & (df["predicted"] == 0)).sum()

    accuracy = (TP + TN) / (TP + TN + FP + FN)
    precision = TP / (TP + FP) if (TP + FP) != 0 else 0
    recall = TP / (TP + FN) if (TP + FN) != 0 else 0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) != 0 else 0

    print("Accuracy:", round(accuracy, 2))
    print("Precision:", round(precision, 2))
    print("Recall:", round(recall, 2))
    print("F1 Score:", round(f1, 2))
