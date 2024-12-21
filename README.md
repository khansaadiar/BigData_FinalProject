# Final_Project-Big_Data

|         Name            |    NRP     |
|-------------------------|------------|
| Muhammad Faishal Rizqy  | 5027221026 |
| Rafif Dhimaz Ardhana    | 5027221066 |
| Khansa Adia Rahma       | 5027221071 |


## Prediksi Biaya Pengiriman eBay
![alt text](images/ebay.png)

Proyek ini bertujuan untuk memprediksi biaya pengiriman menggunakan dataset dari [Kaggle: eBay Delivery Date Prediction](https://www.kaggle.com/datasets/armanaanand/ebay-delivery-date-prediction). Dengan memanfaatkan data historis pengiriman, model ini dirancang untuk memberikan estimasi biaya pengiriman yang akurat. Sistem ini juga dirancang untuk beroperasi secara real-time, memungkinkan pembaruan model yang otomatis dan responsif terhadap perubahan dalam dataset.

### Fitur Utama
- Prediksi Biaya Pengiriman: Model ini mampu memprediksi **shipping fee** berdasarkan data yang diberikan.
- Pengolahan Data Real-time: Menggunakan arsitektur yang efisien untuk **memproses data secara real-time**.
- Otomatisasi Pembaruan Model: Proses pelatihan dan pembaruan model dilakukan secara otomatis saat data baru tersedia, memastikan **model selalu menggunakan data terupdate**.

## Arsitekur
![alt text](images/arsitektur.png)
## Work Flow
- Pengambilan Data:
    - Producer memeriksa dataset CSV secara berkala untuk mendeteksi adanya pembaruan. Jika ada data baru, Producer akan mengirimkannya ke Kafka.
- Pengolahan Data:
    - Kafka topic akan menerima data dari Producer.
    - Consumer akan mengambil data baru dari Kafka dan menyimpannya di MinIO.
- Pelatihan Model:
    - Skrip Python yang menjalankan pelatihan model akan memeriksa keberadaan data baru di MinIO setiap 5 menit.
    - Jika data baru terdeteksi, proses Train and Modelling akan dilakukan untuk memperbarui model dengan data terbaru.
- Prediksi Real-time:
    - UI (antarmuka pengguna) selalu menggunakan model terbaru untuk memprediksi biaya pengiriman. Pengguna dapat mengakses prediksi secara langsung melalui RESTful API yang disediakan oleh aplikasi Flask.
    - **Penyimpanan Data**: Data yang diinput melalui UI dan hasil prediksi akan disimpan ke dalam file CSV untuk dibaca kembali oleh **Producer**, mendukung siklus pembelajaran model yang berkelanjutan.
- Otomatisasi:
    - Seluruh alur kerja diotomatisasi untuk memastikan bahwa model selalu diperbarui dan siap memberikan prediksi yang akurat berdasarkan data terbaru.

### Run program
Gunakan perintah berikut untuk memulai semua komponen program secara bersamaan, termasuk Producer, Kafka, Consumer, dan pelatihan model:
```
python start_all.py
```

**Akses UI:** Setelah semua komponen berjalan, akses antarmuka pengguna melalui ``http://localhost:5000``  untuk melakukan prediksi biaya pengiriman.

### Tampilan Web UI
Input Random Data for Prediction:
![alt text](images/predict1.png)
check the accuracy by adding the weight:
![alt text](images/predict2.png)
now raise the price:
![alt text](images/itemprice.png)
Change the shipment method:
![alt text](images/shipmentmethod.png)
add quantity:
![alt text](images/quantity.png)
change declared handling days:
![alt text](images/declaredhandlingdays.png)
change the distance:
![alt text](images/distance.png)
change category ID:
![alt text](images/category.png)



