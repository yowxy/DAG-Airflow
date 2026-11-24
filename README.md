<div align="center">

<h1>Dokumentasi Projek ETL dan DAG Air Quality Indonesia</h1>

<p>
<img src="https://img.shields.io/badge/Apache%20Airflow-3.4.0-017CEE" alt="Airflow">
<img src="https://img.shields.io/badge/Python-3.12-3776AB" alt="Python">
<img src="https://img.shields.io/badge/PostgreSQL-16-4169E1" alt="PostgreSQL">
<img src="https://img.shields.io/badge/Docker-Compose-2496ED" alt="Docker">
<img src="https://img.shields.io/badge/Pipeline-Success-brightgreen" alt="Status">
</p>

</div>

<h2>1. Ringkasan Projek</h2>
<p>
Projek ini disusun untuk memenuhi evaluasi akhir semester pada mata kuliah Praktikum Pemrograman dengan tema Introduction to Directed Acyclic Graph (DAG) and ETL Workflow. Sistem ini mengimplementasikan alur kerja otomatis untuk mengekstrak data kualitas udara harian di Indonesia, membersihkan dan mentransformasikan metrik polusi, mengagregasikan ringkasan per provinsi, serta memuat data ke dalam data warehouse PostgreSQL secara idempoten.
</p>

<h2>2. Spesifikasi Dataset</h2>
<p>
Dataset yang digunakan mencakup observasi harian indeks kualitas udara stasiun pemantau di berbagai wilayah Indonesia.
</p>
<table border="1" cellpadding="8" cellspacing="0">
<tr>
<th>Atribut</th>
<th>Deskripsi</th>
</tr>
<tr>
<td>Sumber File</td>
<td>data/raw/idn_air_quality_daily.csv</td>
</tr>
<tr>
<td>Jumlah Rekor</td>
<td>48.701 baris data</td>
</tr>
<tr>
<td>Cakupan Wilayah</td>
<td>31 Kota, 29 Provinsi, 7 Kepulauan Utama</td>
</tr>
<tr>
<td>Rentang Waktu</td>
<td>04 Agustus 2022 hingga 21 November 2026</td>
</tr>
<tr>
<td>Parameter Polutan</td>
<td>PM2.5 rata-rata, PM2.5 maksimal, PM10 rata-rata, US AQI rata-rata, US AQI maksimal</td>
</tr>
</table>

<h2>3. Arsitektur dan Tahapan Pipeline DAG</h2>
<p>
Pipeline diatur dalam file DAG bernama <code>etl_air_quality_indonesia</code> yang beroperasi dengan empat tahapan sekuensial:
</p>

<table border="1" cellpadding="8" cellspacing="0">
<tr>
<th>Nama Task</th>
<th>Operator</th>
<th>Fungsi dan Logika Pemrosesan</th>
</tr>
<tr>
<td>extract_raw_data</td>
<td>PythonOperator</td>
<td>Membaca file mentah CSV dari storage, memvalidasi keberadaan 11 kolom wajib, memastikan integritas file dan ketersediaan data.</td>
</tr>
<tr>
<td>transform_air_quality_data</td>
<td>PythonOperator</td>
<td>Menangani 31 nilai kosong pada metrik US AQI menggunakan imputasi median kota, melakukan parsing tanggal (ekstraksi tahun, bulan, kuartal), mengklasifikasikan kategori AQI (Baik, Sedang, Tidak Sehat Kelompok Sensitif, Tidak Sehat, Sangat Tidak Sehat, Berbahaya), membulatkan presisi desimal, menghasilkan data mart agregasi tahunan per provinsi, dan menyimpan hasil ke folder data/processed/.</td>
</tr>
<tr>
<td>load_data_to_warehouse</td>
<td>PythonOperator</td>
<td>Menginisialisasi koneksi database PostgreSQL, membuat skema tabel air_quality_daily dan air_quality_province_summary, serta menulis seluruh dataset bersih secara idempoten.</td>
</tr>
<tr>
<td>verify_warehouse_data</td>
<td>PythonOperator</td>
<td>Melakukan rekonsiliasi data otomatis melalui kueri SQL untuk memvalidasi jumlah baris tersimpan dan integritas sampel data warehouse.</td>
</tr>
</table>

<h2>4. Struktur Direktori</h2>
<table border="1" cellpadding="8" cellspacing="0">
<tr>
<th>Direktori atau File</th>
<th>Peran dalam Sistem</th>
</tr>
<tr>
<td>dags/</td>
<td>Menyimpan kode definisi DAG Airflow (etl_air_quality_dag.py).</td>
</tr>
<tr>
<td>data/raw/</td>
<td>Menyimpan dataset mentah (idn_air_quality_daily.csv).</td>
</tr>
<tr>
<td>data/processed/</td>
<td>Menyimpan output olahan berupa clean_air_quality_daily.csv dan air_quality_province_summary.csv.</td>
</tr>
<tr>
<td>docker-compose.yaml</td>
<td>Konfigurasi orkestrasi layanan Airflow, PostgreSQL, dan Redis dengan mount volume folder data.</td>
</tr>
<tr>
<td>config/</td>
<td>Konfigurasi sistem Airflow lokal.</td>
</tr>
<tr>
<td>logs/</td>
<td>Log eksekusi scheduler, worker, dan task instance Airflow.</td>
</tr>
<tr>
<td>plugins/</td>
<td>Custom plugin pendukung Airflow.</td>
</tr>
<tr>
<td>.env</td>
<td>Konfigurasi environment variable UID sistem (AIRFLOW_UID=1000).</td>
</tr>
<tr>
<td>notebooks/</td>
<td>Menyimpan Jupyter Notebook analisis eksploratif data (eda_air_quality.ipynb).</td>
</tr>
<tr>
<td>reports/figures/</td>
<td>Menyimpan artefak visualisasi grafik hasil analisis (format PNG 300 DPI).</td>
</tr>
<tr>
<td>generate_visualizations.py</td>
<td>Script otomatisasi pembuatan 5 grafik visualisasi data kualitas udara.</td>
</tr>
</table>

<h2>5. Skema Tabel Data Warehouse PostgreSQL</h2>
<p>Tabel 1: air_quality_daily (Tabel Fakta Kualitas Udara Harian)</p>
<table border="1" cellpadding="8" cellspacing="0">
<tr>
<th>Kolom</th>
<th>Tipe Data</th>
<th>Keterangan</th>
</tr>
<tr>
<td>city</td>
<td>TEXT</td>
<td>Nama stasiun kota pemantauan</td>
</tr>
<tr>
<td>province</td>
<td>TEXT</td>
<td>Nama provinsi</td>
</tr>
<tr>
<td>island</td>
<td>TEXT</td>
<td>Nama gugus kepulauan</td>
</tr>
<tr>
<td>date</td>
<td>TEXT</td>
<td>Tanggal observasi (format YYYY-MM-DD)</td>
</tr>
<tr>
<td>pm2_5_mean</td>
<td>FLOAT</td>
<td>Konsentrasi rata-rata harian partikulat PM2.5</td>
</tr>
<tr>
<td>pm2_5_max</td>
<td>FLOAT</td>
<td>Konsentrasi maksimum harian PM2.5</td>
</tr>
<tr>
<td>pm10_mean</td>
<td>FLOAT</td>
<td>Konsentrasi rata-rata harian partikulat PM10</td>
</tr>
<tr>
<td>us_aqi_mean</td>
<td>FLOAT</td>
<td>Nilai rata-rata harian indeks US AQI</td>
</tr>
<tr>
<td>us_aqi_max</td>
<td>FLOAT</td>
<td>Nilai maksimum harian indeks US AQI</td>
</tr>
<tr>
<td>hours</td>
<td>BIGINT</td>
<td>Durasi jam pencatatan aktif sensor per hari</td>
</tr>
<tr>
<td>exceeds_who_pm25</td>
<td>BOOLEAN</td>
<td>Indikator pelampauan batas ambang WHO</td>
</tr>
<tr>
<td>year</td>
<td>BIGINT</td>
<td>Tahun observasi</td>
</tr>
<tr>
<td>month</td>
<td>BIGINT</td>
<td>Bulan observasi</td>
</tr>
<tr>
<td>quarter</td>
<td>BIGINT</td>
<td>Kuartal tahunan observasi</td>
</tr>
<tr>
<td>aqi_category</td>
<td>TEXT</td>
<td>Kategori kualitas udara standar EPA</td>
</tr>
<tr>
<td>is_severe_pollution</td>
<td>BOOLEAN</td>
<td>Penanda status polusi kritis (AQI di atas 150)</td>
</tr>
</table>

<p>Tabel 2: air_quality_province_summary (Tabel Agregasi per Provinsi dan Tahun)</p>
<table border="1" cellpadding="8" cellspacing="0">
<tr>
<th>Kolom</th>
<th>Tipe Data</th>
<th>Keterangan</th>
</tr>
<tr>
<td>province</td>
<td>TEXT</td>
<td>Nama provinsi</td>
</tr>
<tr>
<td>island</td>
<td>TEXT</td>
<td>Nama gugus kepulauan</td>
</tr>
<tr>
<td>year</td>
<td>BIGINT</td>
<td>Tahun agregasi</td>
</tr>
<tr>
<td>total_observations</td>
<td>BIGINT</td>
<td>Total observasi stasiun per tahun</td>
</tr>
<tr>
<td>avg_pm2_5</td>
<td>FLOAT</td>
<td>Rata-rata PM2.5 tahunan</td>
</tr>
<tr>
<td>max_pm2_5</td>
<td>FLOAT</td>
<td>Puncak PM2.5 tertinggi tahunan</td>
</tr>
<tr>
<td>avg_aqi</td>
<td>FLOAT</td>
<td>Rata-rata US AQI tahunan</td>
</tr>
<tr>
<td>max_aqi</td>
<td>FLOAT</td>
<td>Puncak US AQI tertinggi tahunan</td>
</tr>
<tr>
<td>who_exceed_days</td>
<td>BIGINT</td>
<td>Jumlah hari pelampauan ambang WHO</td>
</tr>
<tr>
<td>who_exceed_ratio</td>
<td>FLOAT</td>
<td>Rasio hari pelampauan terhadap total hari</td>
</tr>
</table>

<h2>6. Panduan Menjalankan Sistem</h2>
<p>Langkah 1: Menjalankan Klaster Airflow Menggunakan Docker Compose</p>
<pre><code>docker compose up -d</code></pre>

<p>Langkah 2: Memverifikasi Status Layanan Container</p>
<pre><code>docker compose ps</code></pre>

<p>Langkah 3: Mengaktifkan (Unpause) DAG di Airflow</p>
<pre><code>docker exec projectuas-airflow-worker-1 airflow dags unpause etl_air_quality_indonesia</code></pre>

<p>Langkah 4: Menjalankan Eksekusi DAG Secara Manual</p>
<pre><code>docker exec projectuas-airflow-worker-1 airflow dags trigger etl_air_quality_indonesia</code></pre>

<p>Langkah 5: Memeriksa Hasil Output File dan Data Warehouse</p>
<pre><code>ls -la data/processed/</code></pre>
<pre><code>docker exec projectuas-airflow-worker-1 python -c "import sqlalchemy; engine = sqlalchemy.create_engine('postgresql+psycopg://airflow:airflow@postgres/airflow'); conn = engine.connect(); print('Daily rows:', conn.execute(sqlalchemy.text('SELECT COUNT(1) FROM air_quality_daily;')).scalar()); print('Summary rows:', conn.execute(sqlalchemy.text('SELECT COUNT(1) FROM air_quality_province_summary;')).scalar()); conn.close()"</code></pre>

<h2>7. Hasil Validasi dan Karakteristik Data Engineering</h2>
<table border="1" cellpadding="8" cellspacing="0">
<tr>
<th>Kriteria Rekayasa</th>
<th>Status Pengujian</th>
<th>Hasil Observasi</th>
</tr>
<tr>
<td>Idempotensi Pipeline</td>
<td>Terpenuhi</td>
<td>Eksekusi berulang tidak memicu duplikasi data atau kegagalan transaksi database.</td>
</tr>
<tr>
<td>Kelengkapan Data Harian</td>
<td>Terpenuhi</td>
<td>48.701 baris data berhasil diekstrak, dibersihkan, dan dimuat ke PostgreSQL tanpa anomali.</td>
</tr>
<tr>
<td>Keberhasilan Agregasi</td>
<td>Terpenuhi</td>
<td>145 metrik ringkasan provinsi per tahun berhasil dihitung dan dimuat ke tabel agregasi.</td>
</tr>
<tr>
<td>Integritas Data</td>
<td>Terpenuhi</td>
<td>Imputasi nilai kosong berhasil menyelesaikan 31 anomali AQI tanpa kehilangan observasi.</td>
</tr>
</table>


<h2>8. Exploratory Data Analysis (EDA) dan Visualisasi Data</h2>
<p>
Untuk melengkapi luaran projek, analisis eksploratif dan visualisasi data telah diotomatisasi melalui skrip <code>generate_visualizations.py</code> serta didokumentasikan interaktif dalam <code>notebooks/eda_air_quality.ipynb</code>.
</p>

<table border=1 cellpadding=8 cellspacing=0>
<tr>
<th>File Grafik</th>
<th>Judul Visualisasi</th>
<th>Temuan Kunci</th>
</tr>
<tr>
<td>01_distribusi_kategori_aqi.png</td>
<td>Proporsi Status Kualitas Udara Nasional (Donut Chart)</td>
<td>Mayoritas udara Indonesia berstatus Sedang (56,0%) dan Baik (27,4%), dengan 16,6% berada pada kategori Tidak Sehat hingga Berbahaya.</td>
</tr>
<tr>
<td>02_top_provinsi_aqi.png</td>
<td>5 Provinsi Paling Berpolusi vs 5 Provinsi Terbersih (Bar Chart)</td>
<td>Disparitas tajam antara DKI Jakarta (AQI 153,5 - Sangat Berpolusi) dan Papua Barat Daya (AQI 32,8 - Sangat Sehat).</td>
</tr>
<tr>
<td>03_persentase_pelanggaran_who_per_pulau.png</td>
<td>Pelampauan Batas Aman Standar WHO per Pulau (Bar Chart)</td>
<td>Pulau Jawa menjadi episentrum krisis udara dengan 88,9% hari melampaui batas aman WHO (PM2.5 > 15 µg/m³), berbanding jauh dengan Papua (2,8%).</td>
</tr>
<tr>
<td>04_tren_tahunan_aqi_pm25.png</td>
<td>Tren Kenaikan Polusi Udara Tahunan 2022–2026 (Line Chart)</td>
<td>Terjadi kenaikan bertahap pada konsentrasi polutan PM2.5 dan US AQI dari tahun 2022 hingga 2026.</td>
</tr>
<tr>
<td>05_pola_musiman_polusi_bulanan.png</td>
<td>Pola Musiman Polusi: Kemarau vs Hujan (Line Chart)</td>
<td>Konsentrasi polutan memuncak drastis selama musim kemarau (Juni–Oktober) akibat rendahnya presipitasi curah hujan.</td>
</tr>
</table>

<p>Perintah untuk memperbarui/menghasilkan seluruh visualisasi secara mandiri:</p>
<pre><code>python3 generate_visualizations.py</code></pre>
