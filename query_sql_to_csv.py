import pandas as pd
from sqlalchemy import create_engine

# Buat koneksi ke database
host = "localhost"  # Ganti dengan host database Anda
user = "root"       # Ganti dengan username Anda
password = ""  # Ganti dengan password Anda
database = "paper_id_pbl"   # Ganti dengan nama database Anda

# Koneksi menggunakan sqlalchemy
engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

# Contoh tes koneksi
with engine.connect() as connection:
    print("Connected to the database successfully!")

# SQL query untuk buyer-seller relationships
query_buyer_seller = """
WITH buyer_seller_relationships AS (
    SELECT
        buyer_id,
        seller_id,
        COUNT(dpt_id) AS transaction_count,
        SUM(CASE WHEN is_outlier_iqr = 1 OR anomaly = 1 THEN 1 ELSE 0 END) AS flagged_transaction_count,
        MAX(CASE WHEN is_burst = 1 THEN 1 ELSE 0 END) AS burst_flag,
        MAX(CASE WHEN buyer_id IN (SELECT company_id FROM users WHERE blacklist_account_flag = 1) 
                 OR seller_id IN (SELECT company_id FROM users WHERE blacklist_account_flag = 1) THEN 1 ELSE 0 END) AS involves_blacklisted_user
    FROM transactions
    GROUP BY buyer_id, seller_id
)
SELECT *
FROM buyer_seller_relationships
WHERE flagged_transaction_count > 0 OR involves_blacklisted_user = 1
ORDER BY flagged_transaction_count DESC, transaction_count DESC;
"""

# Eksekusi query dan simpan hasil ke DataFrame
with engine.connect() as connection:
    buyer_seller_df = pd.read_sql_query(query_buyer_seller, connection)

# Simpan hasil ke CSV
buyer_seller_df.to_csv("buyer_seller_relationships.csv", index=False)
print("Buyer-Seller relationships saved to buyer_seller_relationships.csv!")


# SQL query untuk user connections
query_user_connections = """
WITH user_connections AS (
    SELECT
        buyer_id AS user_id,
        COUNT(DISTINCT seller_id) AS unique_partners,
        SUM(CASE WHEN is_outlier_iqr = 1 OR anomaly = 1 THEN 1 ELSE 0 END) AS flagged_transactions
    FROM transactions
    GROUP BY buyer_id
    UNION ALL
    SELECT
        seller_id AS user_id,
        COUNT(DISTINCT buyer_id) AS unique_partners,
        SUM(CASE WHEN is_outlier_iqr = 1 OR anomaly = 1 THEN 1 ELSE 0 END) AS flagged_transactions
    FROM transactions
    GROUP BY seller_id
)
SELECT user_id, SUM(unique_partners) AS total_partners, SUM(flagged_transactions) AS total_flagged
FROM user_connections
GROUP BY user_id
ORDER BY total_flagged DESC, total_partners DESC;
"""

# Eksekusi query dan simpan hasil ke DataFrame
with engine.connect() as connection:
    user_connections_df = pd.read_sql_query(query_user_connections, connection)

# Simpan hasil ke CSV
user_connections_df.to_csv("user_connections.csv", index=False)
print("User connections saved to user_connections.csv!")
