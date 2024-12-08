import pandas as pd
from sqlalchemy import create_engine
import networkx as nx
import matplotlib.pyplot as plt
import os

# Buat koneksi ke database
engine = create_engine('mysql+pymysql://root:@localhost:3306/paper_id_pbl')

# Query untuk mendapatkan data buyer-seller relationship
query = """
SELECT 
    buyer_id AS source, 
    seller_id AS target,
    COUNT(*) AS interaction_count,
    SUM(CASE WHEN suspicious_flag = 1 THEN 1 ELSE 0 END) AS suspicious_interaction
FROM transactions
GROUP BY buyer_id, seller_id;
"""

# Ekstrak data dari database
data = pd.read_sql(query, engine)

# Validasi data
if data.empty:
    print("Data kosong! Periksa query SQL atau tabel transaksi.")
    exit()

# Simpan data ke file CSV untuk referensi
data.to_csv('buyer_seller_network.csv', index=False)
print("Data berhasil diekspor ke buyer_seller_network.csv")

# Membuat graf NetworkX dari data buyer-seller
G = nx.from_pandas_edgelist(
    data,
    source='source',
    target='target',
    edge_attr=['interaction_count', 'suspicious_interaction']
)

# **1. Analisis Graf**
print(f"Jumlah node: {G.number_of_nodes()}")
print(f"Jumlah edge: {G.number_of_edges()}")

# Tambahkan atribut node: apakah node mencurigakan
suspicious_nodes = set(data[data['suspicious_interaction'] > 0]['source']).union(
    set(data[data['suspicious_interaction'] > 0]['target'])
)
nx.set_node_attributes(
    G, {node: "suspicious" if node in suspicious_nodes else "normal" for node in G.nodes}, name='status'
)

# **2. Centrality Analysis**
# Hitung degree centrality
degree_centrality = nx.degree_centrality(G)
central_users = sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)[:10]
print("Top 10 central users (degree centrality):", central_users)

# **3. Community Detection**
# Gunakan greedy modularity untuk mendeteksi komunitas
from networkx.algorithms.community import greedy_modularity_communities
communities = list(greedy_modularity_communities(G))
print(f"Jumlah komunitas terdeteksi: {len(communities)}")

# Mapping komunitas untuk setiap node
community_map = {}
for i, community in enumerate(communities):
    for node in community:
        community_map[node] = i
nx.set_node_attributes(G, community_map, name='community')

# **4. Visualisasi Jaringan**
# Tata letak jaringan
pos = nx.spring_layout(G, seed=42)

# Warnai node berdasarkan komunitas
node_colors = [community_map.get(node, 0) for node in G.nodes]

plt.figure(figsize=(15, 15))
nx.draw_networkx_nodes(
    G, pos, node_size=50, node_color=node_colors, cmap=plt.cm.tab20, alpha=0.8
)
nx.draw_networkx_edges(
    G, pos, edge_color='gray', alpha=0.5
)
plt.title("Buyer-Seller Network (Colored by Community)")
plt.show()

# **5. Visualisasi Subset (Suspicious Interactions Only)**
# Filter edge yang mencurigakan
suspicious_edges = [
    (u, v) for u, v, d in G.edges(data=True) if d.get('suspicious_interaction', 0) > 0
]
G_suspicious = G.edge_subgraph(suspicious_edges)

# Tata letak subset
pos_suspicious = nx.spring_layout(G_suspicious, seed=42)

plt.figure(figsize=(12, 12))
nx.draw_networkx_nodes(
    G_suspicious, pos_suspicious, node_size=50, node_color='red', alpha=0.8
)
nx.draw_networkx_edges(
    G_suspicious, pos_suspicious, edge_color='black', alpha=0.5
)
plt.title("Suspicious Buyer-Seller Network")
plt.show()

# **6. Simpan Output**
# Simpan subset graf ke file GML
output_gml_file = "buyer_seller_network_suspicious.gml"
nx.write_gml(G_suspicious, output_gml_file)
print("Grafik jaringan (subset mencurigakan) berhasil disimpan ke:", os.path.abspath(output_gml_file))

# Simpan hasil centrality ke CSV
centrality_df = pd.DataFrame(central_users, columns=['user_id', 'degree_centrality'])
centrality_df.to_csv('central_users.csv', index=False)
print("Hasil centrality analysis disimpan ke central_users.csv")