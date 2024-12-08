import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Membaca dataset
file_path = "transactions.csv"  # Ganti dengan path file dataset Anda
transactions = pd.read_csv(file_path)

# 2. Konversi kolom datetime ke format datetime
transactions['transaction_created_datetime'] = pd.to_datetime(transactions['transaction_created_datetime'])

# 3. Menentukan cohort_month (bulan transaksi pertama setiap pembeli)
transactions['cohort_month'] = transactions.groupby('buyer_id')['transaction_created_datetime'].transform('min').dt.to_period('M')

# 4. Menentukan transaction_month
transactions['transaction_month'] = transactions['transaction_created_datetime'].dt.to_period('M')

# 5. Menghitung "Months Since First Transaction"
transactions['months_since_first_transaction'] = (
    (transactions['transaction_month'] - transactions['cohort_month']).apply(lambda x: x.n)
)

# 6. Mengelompokkan data untuk cohort analysis
cohort_data = transactions.groupby(['cohort_month', 'months_since_first_transaction']).agg(
    total_transactions=('transaction_amount', 'sum'),
    avg_transaction=('transaction_amount', 'mean'),
    unique_buyers=('buyer_id', 'nunique')
).reset_index()

# 7. Membuat pivot table untuk heatmap (total transaksi)
cohort_pivot = cohort_data.pivot(index='cohort_month', columns='months_since_first_transaction', values='total_transactions')

# 8. Ringkasan Statistik Cohort Analysis
print("\n--- Cohort Analysis Summary Statistics ---\n")
summary_stats = cohort_data.groupby('cohort_month').agg(
    total_transactions=('total_transactions', 'sum'),
    avg_transaction=('avg_transaction', 'mean'),
    unique_buyers=('unique_buyers', 'sum')
).reset_index()
print(summary_stats)

# 9. Analisis pola fraud
fraud_analysis = transactions.groupby(['buyer_id', 'seller_id']).agg(
    total_transactions=('transaction_amount', 'count'),
    flagged_fraud=('anomaly', 'sum'),
    last_transaction=('transaction_created_datetime', 'max'),
    days_inactive=('transaction_created_datetime', lambda x: (x.max() - x.min()).days)
).reset_index()

# Menentukan pembeli dengan pola fraud
fraud_analysis['fraud_flag'] = np.where(
    (fraud_analysis['flagged_fraud'] > 0) & (fraud_analysis['days_inactive'] > 30), 1, 0
)

# Membuat salinan eksplisit untuk menghindari SettingWithCopyWarning
fraudulent_buyers = fraud_analysis[fraud_analysis['fraud_flag'] == 1].copy()

# Mengatasi nilai NaN sebelum agregasi
fraudulent_buyers['days_inactive'] = fraudulent_buyers['days_inactive'].fillna(0)  # Ganti NaN dengan 0
fraudulent_buyers['flagged_fraud'] = fraudulent_buyers['flagged_fraud'].fillna(0)  # Ganti NaN dengan 0
fraudulent_buyers['buyer_id'] = fraudulent_buyers['buyer_id'].fillna("Unknown")  # Ganti NaN dengan "Unknown"

# Ubah tipe data ke tipe yang sesuai untuk memastikan agregasi berjalan
fraudulent_buyers['flagged_fraud'] = fraudulent_buyers['flagged_fraud'].astype(float)
fraudulent_buyers['days_inactive'] = fraudulent_buyers['days_inactive'].astype(float)

# 10. Ringkasan Statistik Fraud Analysis
print("\n--- Fraud Analysis Summary Statistics ---\n")
fraud_summary_stats = pd.DataFrame({
    'total_fraud_transactions': [fraudulent_buyers['flagged_fraud'].sum()],
    'avg_days_inactive': [fraudulent_buyers['days_inactive'].mean()],
    'total_fraud_buyers': [fraudulent_buyers['buyer_id'].nunique()]
})
print(fraud_summary_stats)

# 11. Visualisasi heatmap untuk cohort analysis
plt.figure(figsize=(15, 10))
sns.heatmap(
    cohort_pivot / 1e6,  # Konversi transaksi ke dalam jutaan
    annot=True,
    fmt=".1f",
    cmap="YlGnBu",
    linewidths=0.5,
    cbar_kws={'label': 'Transaction Amount (in Millions)'}
)
plt.title("Cohort Analysis - Total Transactions", fontsize=18)
plt.xlabel("Months Since First Transaction", fontsize=14)
plt.ylabel("Cohort Month", fontsize=14)
plt.tight_layout()
plt.show()