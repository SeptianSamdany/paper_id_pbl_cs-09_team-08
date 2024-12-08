import pandas as pd
import os
os.chdir(r"D:\KULIAH\Bitlabs\PBL\Pycharm")

# Load data dari file CSV
buyer_seller_data = pd.read_csv("buyer_seller_relationships.csv")

# Ambil top 50 transaksi berdasarkan flagged_transaction_count
top_transactions = buyer_seller_data.nlargest(50, 'flagged_transaction_count')

# Tampilkan data untuk memastikan
print(top_transactions.head())

import networkx as nx
import matplotlib.pyplot as plt

# Inisialisasi graph
G = nx.Graph()

# Tambahkan edges ke graph dari subset data
for _, row in top_transactions.iterrows():
    G.add_edge(
        row['buyer_id'],
        row['seller_id'],
        weight=row['flagged_transaction_count']
    )

# Tambahkan atribut penting untuk node (jika perlu)
for node in G.nodes():
    G.nodes[node]['is_blacklisted'] = int(
        node in top_transactions[top_transactions['involves_blacklisted_user'] == 1]['buyer_id'].values or
        node in top_transactions[top_transactions['involves_blacklisted_user'] == 1]['seller_id'].values
    )

# Hitung degree centrality
degree_centrality = nx.degree_centrality(G)

# Sort node berdasarkan centrality
central_users = sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)

# Tampilkan 5 pengguna dengan centrality tertinggi
print("Top 5 Central Users:")
for user, centrality in central_users[:5]:
    print(f"User ID: {user}, Centrality: {centrality}")

# Atur warna node: merah untuk pengguna yang blacklisted, biru untuk lainnya
node_colors = ['red' if G.nodes[node]['is_blacklisted'] else 'blue' for node in G.nodes()]

# Atur ukuran node berdasarkan degree centrality
node_sizes = [500 + 3000 * degree_centrality[node] for node in G.nodes()]

# Visualisasi jaringan
plt.figure(figsize=(15, 10))
pos = nx.spring_layout(G, seed=42)  # Layout jaringan
nx.draw_networkx(
    G, pos,
    with_labels=False,
    node_color=node_colors,
    node_size=node_sizes,
    edge_color="gray",
    alpha=0.8
)

# Tambahkan legenda
plt.title("Buyer-Seller Network (Top 50 Suspicious Transactions)")
plt.show()