#!/usr/bin/env python3
"""
Script Visualisasi dan EDA Kualitas Udara Indonesia (2022 - 2026)
Menghasilkan 5 visualisasi grafik sederhana, rapi, dan mudah dipahami.
"""

import os
os.environ['MPLCONFIGDIR'] = '/tmp'

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Konfigurasi gaya visualisasi
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['font.sans-serif'] = 'DejaVu Sans'
plt.rcParams['font.family'] = 'sans-serif'
plt.rcParams['figure.autolayout'] = True

OUTPUT_DIR = 'reports/figures'
os.makedirs(OUTPUT_DIR, exist_ok=True)

# 1. Load Data Langsung dari Database Warehouse (SQL Query)
import sqlite3
db_path = 'data/processed/air_quality_warehouse.db'
if not os.path.exists(db_path):
    raise FileNotFoundError(f"Database warehouse tidak ditemukan di {db_path}")

conn = sqlite3.connect(db_path)
df_clean = pd.read_sql_query('SELECT * FROM air_quality_daily', conn)
df_sum = pd.read_sql_query('SELECT * FROM air_quality_province_summary', conn)
conn.close()

print(f"Koneksi Database Sukses: {db_path}")
print(f"Total baris diekstrak dari tabel air_quality_daily: {len(df_clean):,}")
print(f"Total baris diekstrak dari tabel air_quality_province_summary: {len(df_sum):,}")

# ==============================================================================
# GRAFIK 1: Distribusi Status Kualitas Udara Nasional (Donut Chart)
# ==============================================================================
plt.figure(figsize=(8, 7), dpi=300)
cat_order = [
    'Baik', 
    'Sedang', 
    'Tidak Sehat Kelompok Sensitif', 
    'Tidak Sehat', 
    'Sangat Tidak Sehat', 
    'Berbahaya'
]
colors = ['#27ae60', '#f39c12', '#e67e22', '#e74c3c', '#8e44ad', '#5d4037']

cat_counts = df_clean['aqi_category'].value_counts()
cat_counts = cat_counts.reindex(cat_order).dropna()

wedges, texts, autotexts = plt.pie(
    cat_counts, 
    labels=cat_counts.index, 
    autopct='%1.1f%%',
    pctdistance=0.8,
    colors=colors,
    startangle=140,
    wedgeprops=dict(width=0.4, edgecolor='white', linewidth=2),
    textprops=dict(fontsize=11)
)

for autotext in autotexts:
    autotext.set_fontsize(10)
    autotext.set_weight('bold')

plt.title('Proporsi Status Kualitas Udara di Indonesia (2022 - 2026)\n(Berdasarkan Standar US AQI)', 
          fontsize=14, pad=20, weight='bold')

plt.tight_layout()
p1 = os.path.join(OUTPUT_DIR, '01_distribusi_kategori_aqi.png')
plt.savefig(p1, bbox_inches='tight')
plt.close()
print(f"Disimpan: {p1}")

# ==============================================================================
# GRAFIK 2: Perbandingan 5 Provinsi Paling Berpolusi vs 5 Provinsi Terbersih
# ==============================================================================
prov_avg = df_clean.groupby(['province', 'island'])['us_aqi_mean'].mean().reset_index()
worst_5 = prov_avg.sort_values(by='us_aqi_mean', ascending=False).head(5)
best_5 = prov_avg.sort_values(by='us_aqi_mean', ascending=True).head(5)

comparison = pd.concat([worst_5, best_5]).sort_values(by='us_aqi_mean', ascending=True)

plt.figure(figsize=(10, 6), dpi=300)
bar_colors = ['#27ae60' if val < 50 else ('#e67e22' if val < 150 else '#e74c3c') for val in comparison['us_aqi_mean']]

bars = plt.barh(comparison['province'], comparison['us_aqi_mean'], color=bar_colors, edgecolor='gray', alpha=0.85)

# Anotasi angka pada batang
for bar in bars:
    w = bar.get_width()
    plt.text(w + 2, bar.get_y() + bar.get_height()/2, f"{w:.1f}", 
             va='center', ha='left', fontsize=10, weight='bold')

plt.axvline(x=50, color='#27ae60', linestyle='--', linewidth=1.2, label='Batas Udara Baik (AQI <= 50)')
plt.axvline(x=100, color='#e67e22', linestyle='--', linewidth=1.2, label='Batas Mulai Tidak Sehat (AQI > 100)')
plt.axvline(x=150, color='#e74c3c', linestyle='--', linewidth=1.2, label='Batas Tidak Sehat (AQI > 150)')

plt.title('5 Provinsi Paling Berpolusi vs 5 Provinsi Terbersih di Indonesia\n(Rata-rata US AQI)', 
          fontsize=13, weight='bold', pad=15)
plt.xlabel('Rata-rata US AQI (Semakin tinggi semakin berpolusi)', fontsize=11, labelpad=10)
plt.xlim(0, 180)
plt.legend(loc='lower right', frameon=True, fontsize=9)

plt.tight_layout()
p2 = os.path.join(OUTPUT_DIR, '02_top_provinsi_aqi.png')
plt.savefig(p2, bbox_inches='tight')
plt.close()
print(f"Disimpan: {p2}")

# ==============================================================================
# GRAFIK 3: Persentase Hari Melebihi Batas Aman WHO (PM2.5 > 15 µg/m³) per Pulau
# ==============================================================================
island_who = df_clean.groupby('island')['exceeds_who_pm25'].mean() * 100
island_who = island_who.sort_values(ascending=False)

plt.figure(figsize=(9, 5.5), dpi=300)
palette = sns.color_palette("Reds_r", n_colors=len(island_who))

bars = plt.bar(island_who.index, island_who.values, color=palette, edgecolor='black', linewidth=0.5, alpha=0.85)

for bar in bars:
    h = bar.get_height()
    plt.text(bar.get_x() + bar.get_width()/2, h + 1.5, f"{h:.1f}%", 
             ha='center', va='bottom', fontsize=10, weight='bold')

plt.title('Ketimpangan Udara Bersih: Persentase Hari Melebihi Batas Standar WHO\n(PM2.5 > 15 µg/m³ Berdasarkan Gugus Pulau)', 
          fontsize=12, weight='bold', pad=15)
plt.ylabel('% Hari Melampaui Batas WHO', fontsize=11)
plt.xlabel('Gugus Kepulauan', fontsize=11, labelpad=10)
plt.ylim(0, 105)
plt.xticks(rotation=20, ha='right', fontsize=10)

plt.tight_layout()
p3 = os.path.join(OUTPUT_DIR, '03_persentase_pelanggaran_who_per_pulau.png')
plt.savefig(p3, bbox_inches='tight')
plt.close()
print(f"Disimpan: {p3}")

# ==============================================================================
# GRAFIK 4: Tren Kualitas Udara Tahunan (2022 - 2026)
# ==============================================================================
yearly = df_clean.groupby('year')[['pm2_5_mean', 'us_aqi_mean']].mean().reset_index()

fig, ax1 = plt.subplots(figsize=(8.5, 5), dpi=300)

color_aqi = '#d35400'
color_pm25 = '#2980b9'

# Line AQI
line1 = ax1.plot(yearly['year'], yearly['us_aqi_mean'], color=color_aqi, marker='o', linewidth=2.5, markersize=8, label='US AQI (Rata-rata)')
ax1.set_xlabel('Tahun Observasi', fontsize=11, labelpad=10)
ax1.set_ylabel('Rata-rata US AQI', color=color_aqi, fontsize=11, weight='bold')
ax1.tick_params(axis='y', labelcolor=color_aqi)
ax1.set_xticks(yearly['year'])

for x, y in zip(yearly['year'], yearly['us_aqi_mean']):
    ax1.annotate(f"{y:.1f}", (x, y), textcoords="offset points", xytext=(0, 10), ha='center', weight='bold', color=color_aqi)

# Line PM2.5 (Twin Axis)
ax2 = ax1.twinx()
line2 = ax2.plot(yearly['year'], yearly['pm2_5_mean'], color=color_pm25, marker='s', linewidth=2.5, markersize=8, linestyle='--', label='PM2.5 (µg/m³)')
ax2.set_ylabel('Konsentrasi PM2.5 (µg/m³)', color=color_pm25, fontsize=11, weight='bold')
ax2.tick_params(axis='y', labelcolor=color_pm25)
ax2.grid(False) # Hindari tabrakan grid

for x, y in zip(yearly['year'], yearly['pm2_5_mean']):
    ax2.annotate(f"{y:.1f}", (x, y), textcoords="offset points", xytext=(0, -18), ha='center', weight='bold', color=color_pm25)

# Batas aman tahunan WHO PM2.5 (15 µg/m³)
ax2.axhline(y=15, color='#27ae60', linestyle=':', label='Batas Aman Harian WHO (15 µg/m³)')

lines = line1 + line2 + [plt.Line2D([0], [0], color='#27ae60', linestyle=':')]
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc='upper left', frameon=True)

plt.title('Tren Kenaikan Polusi Udara di Indonesia (2022 - 2026)', fontsize=13, weight='bold', pad=15)
plt.tight_layout()
p4 = os.path.join(OUTPUT_DIR, '04_tren_tahunan_aqi_pm25.png')
plt.savefig(p4, bbox_inches='tight')
plt.close()
print(f"Disimpan: {p4}")

# ==============================================================================
# GRAFIK 5: Pola Musiman Kualitas Udara (Per Bulan: Kemarau vs Hujan)
# ==============================================================================
df_clean['date'] = pd.to_datetime(df_clean['date'])
df_clean['month'] = df_clean['date'].dt.month

monthly_java = df_clean[df_clean['island'] == 'Java'].groupby('month')['us_aqi_mean'].mean()
monthly_non_java = df_clean[df_clean['island'] != 'Java'].groupby('month')['us_aqi_mean'].mean()

month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'Mei', 'Jun', 'Jul', 'Agu', 'Sep', 'Okt', 'Nov', 'Des']

plt.figure(figsize=(10, 5.5), dpi=300)
plt.plot(month_names, monthly_java.values, marker='o', linewidth=2.5, color='#c0392b', label='Pulau Jawa (Pusat Polusi)')
plt.plot(month_names, monthly_non_java.values, marker='s', linewidth=2, color='#2980b9', linestyle='--', label='Luar Pulau Jawa')

# Highlight musim kemarau
plt.axvspan('Jun', 'Okt', color='#f39c12', alpha=0.15, label='Puncak Musim Kemarau (Polusi Naik)')

for i, txt in enumerate(monthly_java.values):
    plt.annotate(f"{txt:.0f}", (month_names[i], txt), textcoords="offset points", xytext=(0, 8), ha='center', fontsize=9, color='#c0392b', weight='bold')

plt.title('Pola Musiman Polusi Udara: Pengaruh Musim Kemarau vs Musim Hujan\n(Perbandingan Pulau Jawa vs Luar Pulau Jawa)', 
          fontsize=12, weight='bold', pad=15)
plt.xlabel('Bulan', fontsize=11, labelpad=10)
plt.ylabel('Rata-rata US AQI', fontsize=11)
plt.legend(loc='lower left', frameon=True, fontsize=10)

plt.tight_layout()
p5 = os.path.join(OUTPUT_DIR, '05_pola_musiman_polusi_bulanan.png')
plt.savefig(p5, bbox_inches='tight')
plt.close()
print(f"Disimpan: {p5}")

print("\nSemua 5 visualisasi grafik berhasil dibuat dan disimpan di folder:", OUTPUT_DIR)
