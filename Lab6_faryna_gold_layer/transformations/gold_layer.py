from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

# dim_location for unique lat/lon/timezone
@dp.materialized_view(
    name = "dbr_dev.faryna_gold.dim_location",
    comment = "Location dimension - unique geographic/timezone combinations"
)
def dim_locatio():
    df = spark.read.table("dbr_dev.faryna_silver.weather_silver")

    return (
        df.select(
            "latitude",
            "longitude",
            "timezone",
            "timezone_abbreviation",
            "utc_offset_seconds",
            "elevation"
        )
        .distinct()
        .withColumn(
            "location_sk", 
            F.sha2(
                F.concat_ws("|",
                    F.col("latitude").cast("string"),
                    F.col("longitude").cast("string"),
                    F.col("timezone")
                ), 256
            )
        )
        .select(
            "location_sk",
            "latitude",
            "longitude",
            "timezone",
            "timezone_abbreviation",
            "utc_offset_seconds",
            "elevation"
        )
    )

@dp.materialized_view(
    name="dbr_dev.faryna_gold.dim_source",
    comment="Source dimension — unique data source / file combinations"
)
def dim_source():
    df = spark.read.table("dbr_dev.faryna_silver.weather_silver")
 
    return (
        df.select(
            "source",
            "source_file",
            "generationtime_ms"
        )
        .distinct()
        .withColumn(
            "source_sk",
            F.sha2(
                F.concat_ws("|",
                    F.col("source"),
                    F.col("source_file")
                ), 256
            )
        )
        .select(
            "source_sk",
            F.col("source").alias("source_name"),
            "source_file",
            "generationtime_ms"
        )
    )

@dp.materialized_view(
    name="dbr_dev.faryna_gold.fact_weather_hourly",
    comment="Hourly weather fact table — measures joined to location and source dimensions"
)
def fact_weather_hourly():
    silver = spark.read.table("dbr_dev.faryna_silver.weather_silver")
    dim_loc = spark.read.table("dbr_dev.faryna_gold.dim_location")
    dim_src = spark.read.table("dbr_dev.faryna_gold.dim_source")
 
    return (
        silver
        .join(
            dim_loc.select("location_sk", "latitude", "longitude", "timezone"),
            on=["latitude", "longitude", "timezone"],
            how="left"
        )
        .join(
            dim_src.select("source_sk", F.col("source_name").alias("source"), "source_file"),
            on=["source", "source_file"],
            how="left"
        )
        .withColumn(
            "weather_sk",
            F.sha2(
                F.concat_ws("|",
                    F.col("time").cast("string"),
                    F.col("source"),
                    F.col("location_sk")
                ), 256
            )
        )
        .select(
            "weather_sk",
            "location_sk",
            "source_sk",
            "time",
            "load_date",
            F.col("temperature_celsium").alias("temperature_celsius"),
            "humidity_percent",
            F.col("rain_milimiter").alias("rain_mm"),
            F.col("showers_milimiter").alias("showers_mm"),
            F.col("snowfall_milimiter").alias("snowfall_mm"),
            "wind_speed_10m_s",
            "ingested_at"
        )
    )

@dp.materialized_view(
    name="dbr_dev.faryna_gold.agg_weather_weekly",
    comment="Weekly weather aggregates — rolled up from fact_weather_hourly"
)
def agg_weather_weekly():
    fact = spark.read.table("dbr_dev.faryna_gold.fact_weather_hourly")
 
    return (
        fact
        .withColumn(
            "week_start",
            F.date_trunc("week", F.col("time")).cast("date")
        )
        .groupBy("week_start", "location_sk", "source_sk")
        .agg(
            F.avg("temperature_celsius").alias("avg_temperature"),
            F.max("temperature_celsius").alias("max_temperature"),
            F.min("temperature_celsius").alias("min_temperature"),
            F.avg("humidity_percent").alias("avg_humidity"),
            F.sum("rain_mm").alias("total_rain_mm"),
            F.sum("showers_mm").alias("total_showers_mm"),
            F.sum("snowfall_mm").alias("total_snowfall_mm"),
            F.avg("wind_speed_10m_s").alias("avg_wind_speed"),
            F.count("*").alias("hourly_records")
        )
        .orderBy("week_start")
    )
 

