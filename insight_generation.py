import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Membaca dataset
file_path = "transactions.csv"  # Path dataset transactions
transactions = pd.read_csv(file_path)

# 2. Konversi kolom datetime
transactions['transaction_created_datetime'] = pd.to_datetime(transactions['transaction_created_datetime'])

# 3. Menentukan cohort_month dan transaction_month
transactions['cohort_month'] = transactions.groupby('buyer_id')['transaction_created_datetime'].transform('min').dt.to_period('M')
transactions['transaction_month'] = transactions['transaction_created_datetime'].dt.to_period('M')

# 4. Identifikasi transaksi fraud
transactions['is_fraud'] = np.where(transactions['anomaly'] > 0, 1, 0)

# 5. Identifikasi transaksi dengan promosi
transactions['used_promotion'] = np.where(transactions['dpt_promotion_id'].notnull(), 1, 0)

# 6. Analisis pola fraud dalam setiap cohort
cohort_fraud_analysis = transactions.groupby(['cohort_month', 'used_promotion']).agg(
    total_transactions=('transaction_amount', 'count'),
    total_fraud=('is_fraud', 'sum'),
    fraud_rate=('is_fraud', 'mean')
).reset_index()

# 7. Pivot table untuk visualisasi fraud rate
fraud_pivot = cohort_fraud_analysis.pivot(index='cohort_month', columns='used_promotion', values='fraud_rate')

# 8. Analisis dampak promosi pada fraud
promo_fraud_analysis = transactions[transactions['used_promotion'] == 1].groupby('cohort_month').agg(
    promo_transactions=('transaction_amount', 'count'),
    promo_fraud=('is_fraud', 'sum'),
    promo_fraud_rate=('is_fraud', 'mean')
).reset_index()

# 9. Ringkasan statistik untuk fraud setelah promosi
print("\n--- Fraud After Promotion Analysis ---\n")
print(promo_fraud_analysis)

# 10. Visualisasi fraud rate berdasarkan penggunaan promosi
plt.figure(figsize=(12, 8))
sns.heatmap(
    fraud_pivot * 100,  # Konversi ke persentase
    annot=True,
    fmt=".1f",
    cmap="Reds",
    linewidths=0.5,
    cbar_kws={'label': 'Fraud Rate (%)'}
)
plt.title("Fraud Rate by Cohort and Promotion Usage", fontsize=16)
plt.xlabel("Used Promotion (0 = No, 1 = Yes)", fontsize=14)
plt.ylabel("Cohort Month", fontsize=14)
plt.tight_layout()
plt.show()

# 11. Visualisasi dampak promosi pada fraud
plt.figure(figsize=(10, 6))
plt.bar(
    promo_fraud_analysis['cohort_month'].astype(str),
    promo_fraud_analysis['promo_fraud_rate'] * 100,
    color='tomato',
    alpha=0.8
)
plt.title("Fraud Rate in Transactions Using Promotions", fontsize=16)
plt.xlabel("Cohort Month", fontsize=14)
plt.ylabel("Fraud Rate (%)", fontsize=14)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()