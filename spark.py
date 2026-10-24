from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import window, col, count, when, round
from pyspark.sql.types import StructType, StructField, StringType, TimestampType



spark = SparkSession.builder \
    .master("local[2]") \
    .appName("ParkingStreamAnalysis") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")



schema = StructType([
    StructField("sensor_id",  StringType(),    True),
    StructField("zona",       StringType(),    True),
    StructField("lokacija",   StringType(),    True),
    StructField("status",     StringType(),    True),
    StructField("vozilo_tip", StringType(),    True),
    StructField("placanje",   StringType(),    True),
    StructField("timestamp",  StringType(),    True),
])



raw_df = spark.readStream \
    .format("json") \
    .schema(schema) \
    .option("maxFilesPerTrigger", 1) \
    .load("stream_data/")




cleaned_df = raw_df.withColumn(
    "timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss")
)


cleaned_df = cleaned_df.filter(
    col("sensor_id").isNotNull() & col("zona").isNotNull()
)



PROZOR = "1 minute"
WATERMARK = "30 seconds"

windowed_df = cleaned_df.withWatermark("timestamp", WATERMARK) \
    .groupBy(
        window(col("timestamp"), PROZOR),
        col("zona")
    )



metrika1_df = windowed_df.agg(
    count(when(col("status") == "zauzeto",  1)).alias("zauzeto"),
    count(when(col("status") == "slobodno", 1)).alias("slobodno"),
    count("*").alias("ukupno")
).withColumn(
    "popunjenost_procent",
    round((col("zauzeto") / col("ukupno")) * 100, 2)
).select(
    col("window.start").alias("prozor_start"),
    col("window.end").alias("prozor_end"),
    col("zona"),
    col("zauzeto"),
    col("slobodno"),
    col("ukupno"),
    col("popunjenost_procent")
)


metrika2_df = cleaned_df \
    .filter(col("vozilo_tip").isNotNull()) \
    .withWatermark("timestamp", WATERMARK) \
    .groupBy(
        window(col("timestamp"), PROZOR),
        col("vozilo_tip")
    ).agg(
        count("*").alias("broj_vozila")
    ).select(
        col("window.start").alias("prozor_start"),
        col("window.end").alias("prozor_end"),
        col("vozilo_tip"),
        col("broj_vozila")
    )



metrika3_df = cleaned_df \
    .filter(col("placanje").isNotNull()) \
    .withWatermark("timestamp", WATERMARK) \
    .groupBy(
        window(col("timestamp"), PROZOR),
        col("placanje")
    ).agg(
        count("*").alias("broj_transakcija")
    ).select(
        col("window.start").alias("prozor_start"),
        col("window.end").alias("prozor_end"), 
        col("placanje"),
        col("broj_transakcija")
    )



upozorenja_df = metrika1_df \
    .filter(col("popunjenost_procent") >= 90) \
    .select(
        col("prozor_start"),
        col("prozor_end"),
        col("zona"),
        col("popunjenost_procent"),
        F.lit("⚠️  UPOZORENJE: Zona skoro puna!").alias("upozorenje")
    )



query1 = metrika1_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Metrika1_ZauzetostPoZoni") \
    .trigger(processingTime="5 seconds") \
    .start()

query2 = metrika2_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Metrika2_TipoviVozila") \
    .trigger(processingTime="5 seconds") \
    .start()

query3 = metrika3_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Metrika3_TransakcijePoPlacanju") \
    .trigger(processingTime="5 seconds") \
    .start()

query_upozorenja = upozorenja_df.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", False) \
    .queryName("Detekcija_PunaZona") \
    .trigger(processingTime="5 seconds") \
    .start()



query1_csv = metrika1_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "izlaz/metrika1_zona") \
    .option("checkpointLocation", "checkpoint/metrika1") \
    .option("header", True) \
    .trigger(processingTime="5 seconds") \
    .start()

query2_csv = metrika2_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "izlaz/metrika2_vozila") \
    .option("checkpointLocation", "checkpoint/metrika2") \
    .option("header", True) \
    .trigger(processingTime="5 seconds") \
    .start()

query3_csv = metrika3_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "izlaz/metrika3_placanje") \
    .option("checkpointLocation", "checkpoint/metrika3") \
    .option("header", True) \
    .trigger(processingTime="5 seconds") \
    .start()


query_raw_csv = cleaned_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "izlaz/sirovi_eventi") \
    .option("checkpointLocation", "checkpoint/sirovi") \
    .option("header", True) \
    .trigger(processingTime="5 seconds") \
    .start()



print("=" * 55)
print("  Parking Spark Streaming — pokrenut")
print("  Čita iz: stream_data/")
print("  Čuva u:  izlaz/")
print("  Zaustavi sa Ctrl+C")
print("=" * 55)

spark.streams.awaitAnyTermination()