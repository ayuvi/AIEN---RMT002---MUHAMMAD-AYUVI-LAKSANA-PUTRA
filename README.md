# Pipeline Analitik Data Ritel Omnichannel

## Tentang Project

Project ini dibuat untuk mengolah data ritel dari berbagai sumber menjadi data yang lebih rapi dan siap digunakan untuk analisis bisnis.

Pipeline menggunakan PostgreSQL untuk menyimpan dan mengolah data, serta Apache Airflow untuk menjalankan dan mengatur urutan proses secara otomatis.

## Alur Pipeline

Pipeline dibagi menjadi tiga tahap utama:

1. **Bronze**  
   Menyimpan data mentah dari file sumber sebagai data awal sebelum diproses lebih lanjut.

2. **Silver**  
   Membersihkan dan mengolah data agar lebih terstruktur dan siap digunakan pada tahap analisis.

3. **Gold**  
   Menghasilkan tabel ringkasan yang dapat digunakan untuk kebutuhan laporan dan analisis bisnis.

Apache Airflow digunakan untuk mengatur urutan proses dari awal hingga akhir. Pipeline juga dilengkapi pemeriksaan kualitas data untuk memastikan pemeriksaan yang diterapkan berhasil sebelum proses dilanjutkan.

## Hasil Pengolahan Data

Pipeline menghasilkan lima tabel utama pada tahap Gold:

- `order_360` — data pesanan untuk analisis transaksi.
- `customer_daily` — ringkasan data pelanggan harian.
- `product_daily` — ringkasan performa produk harian.
- `channel_campaign_daily` — ringkasan performa kanal penjualan dan kampanye.
- `executive_kpis_daily` — ringkasan indikator kinerja bisnis harian.

Tabel-tabel tersebut disiapkan agar dapat digunakan untuk analisis lebih lanjut dan mendukung kebutuhan pelaporan bisnis.

## Teknologi yang Digunakan

- Python untuk proses pengolahan data.
- PostgreSQL untuk penyimpanan dan pengolahan data.
- Apache Airflow untuk mengatur jalannya pipeline.
- Docker untuk menjalankan lingkungan aplikasi.

## Menjalankan Project

Pastikan Docker dan file konfigurasi lingkungan lokal sudah disiapkan. Sebelum menjalankan pipeline, konfigurasi Docker Compose dapat diperiksa menggunakan perintah berikut:

```bash
docker compose -f docker-compose.airflow.yaml config --quiet
```

Gunakan file `.env` untuk menyimpan konfigurasi lokal yang diperlukan. Jangan mengunggah file `.env` atau kata sandi ke GitHub.