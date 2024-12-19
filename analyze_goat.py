import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round, month, year, sum as spark_sum, count, when, row_number
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
import os

# Konfigurasi logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    try:
        # Inisialisasi SparkSession
        logger.info("Inisialisasi SparkSession...")
        spark = SparkSession.builder \
            .appName("Goat Sales Analysis") \
            .getOrCreate()

        # Load data dari masing-masing database
        logger.info("Membaca data dari database PostgreSQL...")
        db_credentials = {
            "url_transaksi": os.getenv("DB_URL_TRANSAKSI", "jdbc:postgresql://192.168.84.195:5432/penjualan_kambing"),
            "url_master": os.getenv("DB_URL_MASTER", "jdbc:postgresql://192.168.84.195:5432/master_data"),
            "output_db": os.getenv("DB_URL_OUTPUT", "jdbc:postgresql://192.168.84.195:5432/analyze_result"),
            "driver": "org.postgresql.Driver",
            "user": os.getenv("DB_USER", "postgres"),
            "password": os.getenv("DB_PASSWORD", "barubaru")
        }

        # Validasi koneksi database
        logger.info("Memastikan driver JDBC tersedia...")
        if not db_credentials["driver"]:
            raise ValueError("Driver PostgreSQL tidak ditemukan. Pastikan driver telah ditambahkan ke classpath.")

        logger.info("Membaca data transaksi...")
        data_transaksi = spark.read.format("jdbc").options(
            url=db_credentials["url_transaksi"],
            driver=db_credentials["driver"],
            dbtable="data_transaksi",
            user=db_credentials["user"],
            password=db_credentials["password"]
        ).load()

        logger.info("Membaca master pembeli...")
        master_pembeli = spark.read.format("jdbc").options(
            url=db_credentials["url_master"],
            driver=db_credentials["driver"],
            dbtable="master_pembeli",
            user=db_credentials["user"],
            password=db_credentials["password"]
        ).load()

        logger.info("Membaca master kambing...")
        master_kambing = spark.read.format("jdbc").options(
            url=db_credentials["url_master"],
            driver=db_credentials["driver"],
            dbtable="master_kambing",
            user=db_credentials["user"],
            password=db_credentials["password"]
        ).load()

        logger.info("Membaca master pembayaran...")
        master_pembayaran = spark.read.format("jdbc").options(
            url=db_credentials["url_master"],
            driver=db_credentials["driver"],
            dbtable="master_pembayaran",
            user=db_credentials["user"],
            password=db_credentials["password"]
        ).load()

        logger.info("Membaca master dan transaksi selesai...")

# Analisis 1: Pola penjualan berdasarkan metode transaksi, usia pembeli, dan status transaksi
        # Menggabungkan tabel
        logger.info("Menggabungkan table...")
        joined_df = data_transaksi \
            .join(master_pembeli, data_transaksi["nama"] == master_pembeli["nama"], "inner") \
            .join(master_pembayaran, data_transaksi["id"] == master_pembayaran["id"], "inner") \
            .join(master_kambing, data_transaksi["jenis_kambing"] == master_kambing["jenis_kambing"], "inner")

        # Menambahkan kolom ket_usia
        logger.info("Menambahkan kolom keterangan usia...")
        joined_df = joined_df.withColumn(
            "ket_usia",
            when((col("umur") >= 0) & (col("umur") <= 20), "Muda")
            .when((col("umur") >= 21) & (col("umur") <= 35), "Dewasa")
            .when((col("umur") >= 36) & (col("umur") <= 49), "Tua")
            .otherwise("Lansia")
        )

        # Menghitung total_transaksi dan menambahkan ROW_NUMBER
        logger.info("Menghitung total transaksi...")
        window_spec = Window.partitionBy("kabupaten", "ket_usia").orderBy(col("total_transaksi").desc())
        result_df = joined_df.groupBy("kabupaten", "ket_usia", "metode_transaksi", "metode_pembayaran", "status_transaksi") \
            .agg(count("*").alias("total_transaksi")) \
            .withColumn("rn", row_number().over(window_spec))

        # Memfilter hasil untuk rn = 1
        final_df = result_df.filter(col("rn") == 1).drop("rn")

        # Mengurutkan hasil berdasarkan kabupaten dan ket_usia
        logger.info("Mengurutkan hasil berdasarkan usia dan kabupaten...")
        final_df = final_df.orderBy("kabupaten", "ket_usia")

        # Menampilkan hasil
        final_df.show()

        # Menyimpan hasil analisis metode pembayaran terbanyak
        logger.info("Menyimpan hasil analisis metode pembayaran terbanyak...")
        final_df.write.csv("/home/hadoop/hadoop/data/most_common_payment_method.csv", header=True, mode="overwrite")
        logger.info("Proses CSV selesai...")
        final_df.write.format("jdbc").options(
            url=db_credentials["output_db"],
            driver=db_credentials["driver"],
            dbtable="most_common_payment_method",
            user=db_credentials["user"],
            password=db_credentials["password"]
        ).mode("overwrite").save()

        logger.info("Hasil analisis berhasil disimpan.")

# Analisis 2: Tren penjualan per bulan di tahun 2024
        logger.info("Melakukan analisis tren penjualan per bulan pada tahun 2024...")
        # Filter data tahun 2024
        analisis2 = data_transaksi.alias("dt") \
            .join(master_kambing.alias("mk"), col("dt.jenis_kambing") == col("mk.jenis_kambing")) \
            .join(master_pembeli.alias("mb"), col("dt.nama") == col("mb.nama")) \
            .filter(year(col("dt.tanggal_pembelian")) == 2024)

        if analisis2.count() == 0:
            logger.warning("Tidak ada data untuk tahun 2024. Skipping analysis 2.")
        else:
            # Grouping dan agregasi dengan filter nilai positif
            analisis2 = analisis2.groupBy(
                month(col("dt.tanggal_pembelian")).alias("bulan"),
                "mb.kabupaten",
                "dt.jenis_kambing"
            ).agg(
                spark_sum("dt.jumlah_kambing").alias("jumlah_kambing"),
                round(spark_sum("dt.total_penjualan"), 0).cast("int").alias("total_penjualan")
            ).filter(col("total_penjualan") > 0)  # Hanya nilai positif
            
            # Menampilkan hasil agregasi
            analisis2.show()

            # Menentukan kambing yang paling banyak dibeli
            window_spec = Window.partitionBy("bulan", "kabupaten").orderBy(col("jumlah_kambing").desc())

            # Menambahkan peringkat dan filter hanya rank 1
            analisis2_ranked = analisis2.withColumn("rank", row_number().over(window_spec))
            top_kambing_per_bulan = analisis2_ranked.filter(col("rank") == 1).drop("rank")

            # Urutkan hasil berdasarkan bulan dan kabupaten
            top_kambing_per_bulan = top_kambing_per_bulan.orderBy("bulan", "kabupaten")

            # Menampilkan kesimpulan
            top_kambing_per_bulan.show()

        logger.info("Menyimpan hasil analisis tren penjualan per bulan pada tahun 2024...")
        analisis2.write.csv("/home/hadoop/hadoop/data/sales_trends.csv", header=True, mode="overwrite")
        analisis2.write.format("jdbc").options(
            url=db_credentials["output_db"],
            driver=db_credentials["driver"],
            dbtable="sales_trends",
            user=db_credentials["user"],
            password=db_credentials["password"]
        ).mode("overwrite").save()

        # Analisis 3: Rata-rata harga kambing per jenis
        logger.info("Melakukan analisis rata-rata harga kambing...")
        analisis3 = master_kambing.groupBy("jenis_kambing").agg(round(avg("harga_jual_kambing_"), 0).alias("rata_rata_harga_bulat"))
        # Mengonversi kolom hasil pembulatan ke integer
        analisis3 = analisis3.withColumn("rata_rata_harga", col("rata_rata_harga_bulat").cast("int")).drop("rata_rata_harga_bulat")

        logger.info("Menyimpan hasil analisis rata-rata harga kambing...")
        analisis3.write.csv("/home/hadoop/hadoop/data/average_price.csv", header=True, mode="overwrite")
        analisis3.write.format("jdbc").options(
            url=db_credentials["output_db"],
            driver=db_credentials["driver"],
            dbtable="average_price",
            user=db_credentials["user"],
            password=db_credentials["password"]
        ).mode("overwrite").save()

    except AnalysisException as e:
        logger.error(f"AnalysisException terjadi pada tabel atau kolom tertentu: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Kesalahan tidak terduga: {e}", exc_info=True)
    finally:
        # Berhenti SparkSession
        logger.info("Menghentikan SparkSession...")
        spark.stop()

if __name__ == "__main__":
    main()