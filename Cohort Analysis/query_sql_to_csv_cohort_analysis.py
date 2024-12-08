import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

# Ganti dengan kredensial database Anda
db_host = "localhost"
db_name = "paper_id_pbl"
db_user = "root"
db_password = ""

# Membuat koneksi ke database MySQL
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}/{db_name}')

# Fungsi untuk mengambil data menggunakan query SQL dan mengembalikan DataFrame
def get_data_from_query(query):
    return pd.read_sql(query, engine)

# Query 1: Mengambil transaksi pertama untuk setiap pembeli
query_first_transaction = """
SELECT buyer_id,
       MIN(transaction_created_datetime) AS first_transaction_date
FROM transactions
GROUP BY buyer_id;
"""

# Query 2: Mengukur aktivitas pembeli dari waktu ke waktu
query_activity_over_time = """
SELECT t.buyer_id,
       MIN(t.transaction_created_datetime) AS first_transaction_date,
       COUNT(t.dpt_id) AS total_transactions,
       EXTRACT(YEAR FROM t.transaction_created_datetime) - EXTRACT(YEAR FROM ft.first_transaction_date) AS year_diff,
       EXTRACT(MONTH FROM t.transaction_created_datetime) - EXTRACT(MONTH FROM ft.first_transaction_date) AS month_diff
FROM transactions t
JOIN (SELECT buyer_id, MIN(transaction_created_datetime) AS first_transaction_date
      FROM transactions
      GROUP BY buyer_id) ft ON t.buyer_id = ft.buyer_id
GROUP BY t.buyer_id, first_transaction_date
ORDER BY first_transaction_date, month_diff;
"""

# Query 3: Identifikasi pembeli yang terlibat dalam transaksi yang terindikasi penipuan setelah periode tertentu
query_flagged_transactions = """
SELECT t.buyer_id,
       COUNT(t.dpt_id) AS total_transactions,
       SUM(CASE WHEN t.is_outlier_iqr = 1 OR t.anomaly = 1 THEN 1 ELSE 0 END) AS flagged_transactions
FROM transactions t
JOIN (SELECT buyer_id, MIN(transaction_created_datetime) AS first_transaction_date
      FROM transactions
      GROUP BY buyer_id) ft ON t.buyer_id = ft.buyer_id
WHERE t.transaction_created_datetime > ft.first_transaction_date
  AND TIMESTAMPDIFF(MONTH, ft.first_transaction_date, t.transaction_created_datetime) > 3
GROUP BY t.buyer_id
ORDER BY flagged_transactions DESC;
"""

# Query 4: Pembeli yang berinteraksi lebih dari sekali dengan penjual yang sama
query_repeated_seller_interactions = """
SELECT t.buyer_id,
       t.seller_id,
       COUNT(t.dpt_id) AS total_transactions_with_seller
FROM transactions t
GROUP BY t.buyer_id, t.seller_id
HAVING COUNT(t.dpt_id) > 1
ORDER BY total_transactions_with_seller DESC;
"""

# Mengambil data untuk setiap query
first_transaction_df = get_data_from_query(query_first_transaction)
activity_over_time_df = get_data_from_query(query_activity_over_time)
flagged_transactions_df = get_data_from_query(query_flagged_transactions)
repeated_seller_interactions_df = get_data_from_query(query_repeated_seller_interactions)

# Menyimpan hasil ke dalam file CSV
first_transaction_df.to_csv('cohort_first_transaction.csv', index=False)
activity_over_time_df.to_csv('cohort_activity_over_time.csv', index=False)
flagged_transactions_df.to_csv('flagged_transactions.csv', index=False)
repeated_seller_interactions_df.to_csv('repeated_seller_interactions.csv', index=False)

print("Cohort analysis selesai dan disimpan ke dalam file CSV.")

# Menutup koneksi
engine.dispose()