import subprocess
import time
import boto3

# Daftar file Python yang akan dijalankan
scripts = [
    'producer.py',
    'consumer.py'
]

# Path ke app.py
app_script = '/web_ui/app.py'

# Fungsi untuk menjalankan setiap script
def start_scripts(scripts):
    processes = []
    for script in scripts:
        process = subprocess.Popen(['python', script])
        processes.append(process)
        time.sleep(2)  # Penundaan bisa disesuaikan jika perlu
    return processes

# Fungsi untuk mengecek apakah semua batch sudah tersedia di MinIO
def check_all_batches_ready(bucket_name='ebay', expected_batches=8):
    s3_client = boto3.client(
        's3',
        endpoint_url='http://localhost:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )
    
    while True:
        response = s3_client.list_objects_v2(Bucket=bucket_name)
        files = [obj['Key'] for obj in response.get('Contents', []) if 'batch_' in obj['Key']]
        
        if len(files) >= expected_batches:
            return True  # Semua batch telah tersedia
        time.sleep(10)  # Tunggu sebelum cek lagi

if __name__ == "__main__":
    try:
        print("Memulai Producer dan Consumer...")
        processes = start_scripts(scripts)

        # Tunggu sampai semua batch tersedia di MinIO
        print("Menunggu semua batch di MinIO...")
        if check_all_batches_ready(expected_batches=8):
            print("Semua batch sudah siap. Memulai Training dan App...")
            # Jalankan train.py dan app.py setelah data siap
            training_process = subprocess.Popen(['python', 'train.py'])

            # Memberikan waktu tambahan agar aplikasi web dapat terhubung dengan benar
            time.sleep(5)  # Tunggu beberapa detik sebelum memulai app.py

            # Jalankan app.py
            app_process = subprocess.Popen(['python', app_script])

            # Tunggu sampai app.py selesai berjalan
            app_process.wait()  # Tunggu sampai app.py selesai (untuk mengecek kalau dia aktif)

            # Tunggu sampai training selesai
            training_process.wait()

    except KeyboardInterrupt:
        print("Menghentikan semua script...")
        for process in processes:
            process.terminate()
