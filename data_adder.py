import pandas as pd
import random
import time
from datetime import datetime

# Konfigurasi
CSV_PATH = 'train_data.csv'
MAX_RECORDS = 500000  # Batas maksimum jumlah record

def generate_random_date(start, end):
    return start + (end - start) * random.random()

def add_random_data(batch_size=1000):
    # Membaca data yang ada
    df = pd.read_csv(CSV_PATH)

    # Menentukan rentang tanggal
    start_date = datetime(2018, 1, 1)
    end_date = datetime(2023, 12, 31)

    # Menyimpan data baru dalam list
    new_data_list = []

    for _ in range(batch_size):
        new_data = {
            'b2c_c2c': random.randint(0, 1),
            'seller_id': round(random.uniform(1, 10000000), 1),
            'declared_handling_days': random.randint(1, 5),
            'acceptance_scan_timestamp': generate_random_date(start_date, end_date).isoformat() + '-07:00',
            'shipment_method_id': random.randint(0, 5),
            'shipping_fee': round(random.uniform(1.0, 10.0), 2),
            'carrier_min_estimate': random.randint(1, 7),
            'carrier_max_estimate': random.randint(3, 10),
            'item_zip': random.randint(10000, 99999),
            'buyer_zip': round(random.uniform(10000, 99999), 1),
            'category_id': random.randint(0, 50),
            'item_price': round(random.uniform(5.0, 500.0), 2),
            'quantity': random.randint(1, 10),
            'payment_datetime': generate_random_date(start_date, end_date).isoformat() + '-07:00',
            'delivery_date': generate_random_date(start_date, end_date).date(),
            'weight': round(random.uniform(0.0, 10.0), 6),
            'package_size': random.randint(1, 10),
            'record_number': len(df) + 1,
            'carrier_average_estimate': round(random.uniform(1.0, 10.0), 2),
            'zip_distance': round(random.uniform(1.0, 100.0), 2),
            'acceptance_date': generate_random_date(start_date, end_date).date(),
            'payment_date': generate_random_date(start_date, end_date).date()
        }
        new_data_list.append(new_data)

    # Mengubah new_data_list menjadi DataFrame
    new_df = pd.DataFrame(new_data_list)

    # Menggabungkan DataFrame yang ada dengan DataFrame baru
    df = pd.concat([df, new_df], ignore_index=True)

    # Batasan jumlah record
    if len(df) > MAX_RECORDS:
        df = df.iloc[:MAX_RECORDS]  # Mengambil hanya sampai batas maksimal

    # Menyimpan kembali ke CSV
    df.to_csv(CSV_PATH, index=False)

if __name__ == "__main__":
    while True:
        add_random_data(batch_size=1000)  # Menambah 1k data per iterasi
        time.sleep(10)  # Tambahkan data baru setiap 10 detik