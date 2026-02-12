import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import java.util.Properties
import java.sql.{Connection, DriverManager}
object LoadFactTrip {

  /** Ajoute une colonne si elle n'existe pas (ex: airport_fee, cbd_congestion_fee) */
  def withColIfMissing(df: DataFrame, colName: String, exprCol: org.apache.spark.sql.Column): DataFrame = {
    if (df.columns.contains(colName)) df else df.withColumn(colName, exprCol)
  }

  def main(args: Array[String]): Unit = {
    val year = sys.env.getOrElse("YEAR", "2025")
    val inputPath = sys.env.getOrElse(
      "INPUT_PATH",
      s"s3a://nyc-raw/yellow_tripdata/year=$year/month=*"
    )
    // --- Postgres ---
    val jdbcUrl = sys.env.getOrElse("JDBC_URL", "jdbc:postgresql://localhost:5432/dw")
    val jdbcUser = sys.env.getOrElse("JDBC_USER", "dw")
    val jdbcPassword = sys.env.getOrElse("JDBC_PASSWORD", "dw")
    val targetTable = sys.env.getOrElse("TARGET_TABLE", "dw.fact_trip")

    val spark = SparkSession
      .builder()
      .appName("Data Ingestion")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()
    // Read parquet
    var df = spark.read.parquet(inputPath)

    // Colonnes parfois absentes selon versions
    df = withColIfMissing(df, "airport_fee", lit(0.0))
    df = withColIfMissing(df, "cbd_congestion_fee", lit(0.0))

    // Timestamps
    val pickupTs  = col("tpep_pickup_datetime").cast("timestamp")
    val dropoffTs = col("tpep_dropoff_datetime").cast("timestamp")

    val puDateKey = date_format(pickupTs, "yyyyMMdd").cast("int")
    val doDateKey = date_format(dropoffTs, "yyyyMMdd").cast("int")
    val puTimeKey = date_format(pickupTs, "HHmmss").cast("int")
    val doTimeKey = date_format(dropoffTs, "HHmmss").cast("int")

    // Normalisation store_and_fwd_flag (NULL -> N) et garder 1 char
    val saf = substring(coalesce(col("store_and_fwd_flag").cast("string"), lit("N")), 1, 1)

    // Sélection vers ta fact (NE PAS inclure trip_id)
    val fact = df
      .withColumn("pickup_datetime", pickupTs)
      .withColumn("dropoff_datetime", dropoffTs)
      .withColumn("pu_date_key", puDateKey)
      .withColumn("do_date_key", doDateKey)
      .withColumn("pu_time_key", puTimeKey)
      .withColumn("do_time_key", doTimeKey)
      .withColumn("store_and_fwd_flag_norm", saf)
      // Filtres pour passer les CHECK + FK NOT NULL
      .filter(col("pickup_datetime").isNotNull && col("dropoff_datetime").isNotNull)
      .filter(col("dropoff_datetime") >= col("pickup_datetime"))                       // ck_dropoff_after_pickup_test
      .filter(col("VendorID").isNotNull && col("RatecodeID").isNotNull && col("payment_type").isNotNull)
      .filter(col("PULocationID").isNotNull && col("DOLocationID").isNotNull)
      .filter(col("pu_date_key").between(lit(s"${year}0101").cast("int"), lit(s"${year}1231").cast("int")))
      // Eviter les valeurs négatives (CHECK >= 0)
      .filter(coalesce(col("passenger_count"), lit(0)) >= 0)
      .filter(coalesce(col("trip_distance"), lit(0.0)) >= 0.0)
      .filter(coalesce(col("fare_amount"), lit(0.0)) >= 0.0)
      .filter(coalesce(col("extra"), lit(0.0)) >= 0.0)
      .filter(coalesce(col("mta_tax"), lit(0.0)) >= 0.0)
      .filter(coalesce(col("tip_amount"), lit(0.0)) >= 0.0)
      .filter(coalesce(col("tolls_amount"), lit(0.0)) >= 0.0)
      .filter(coalesce(col("improvement_surcharge"), lit(0.0)) >= 0.0)
      .filter(coalesce(col("congestion_surcharge"), lit(0.0)) >= 0.0)
      .filter(coalesce(col("airport_fee"), lit(0.0)) >= 0.0)
      .filter(coalesce(col("cbd_congestion_fee"), lit(0.0)) >= 0.0)
      .filter(coalesce(col("total_amount"), lit(0.0)) >= 0.0)
      .select(
        col("VendorID").cast("smallint").as("vendor_id"),
        col("RatecodeID").cast("smallint").as("ratecode_id"),
        col("payment_type").cast("smallint").as("payment_type_id"),
        col("store_and_fwd_flag_norm").cast("string").as("store_and_fwd_flag"),

        col("PULocationID").cast("int").as("pu_location_id"),
        col("DOLocationID").cast("int").as("do_location_id"),

        col("pu_date_key").cast("int"),
        col("do_date_key").cast("int"),
        col("pu_time_key").cast("int"),
        col("do_time_key").cast("int"),

        col("pickup_datetime"),
        col("dropoff_datetime"),

        coalesce(col("passenger_count"), lit(0)).cast("smallint").as("passenger_count"),
        coalesce(col("trip_distance"), lit(0.0)).cast("decimal(10,3)").as("trip_distance"),
        coalesce(col("fare_amount"), lit(0.0)).cast("decimal(10,2)").as("fare_amount"),
        coalesce(col("extra"), lit(0.0)).cast("decimal(10,2)").as("extra"),
        coalesce(col("mta_tax"), lit(0.0)).cast("decimal(10,2)").as("mta_tax"),
        coalesce(col("tip_amount"), lit(0.0)).cast("decimal(10,2)").as("tip_amount"),
        coalesce(col("tolls_amount"), lit(0.0)).cast("decimal(10,2)").as("tolls_amount"),
        coalesce(col("improvement_surcharge"), lit(0.0)).cast("decimal(10,2)").as("improvement_surcharge"),
        coalesce(col("congestion_surcharge"), lit(0.0)).cast("decimal(10,2)").as("congestion_surcharge"),
        coalesce(col("airport_fee"), lit(0.0)).cast("decimal(10,2)").as("airport_fee"),
        coalesce(col("cbd_congestion_fee"), lit(0.0)).cast("decimal(10,2)").as("cbd_congestion_fee"),
        coalesce(col("total_amount"), lit(0.0)).cast("decimal(10,2)").as("total_amount")
      )
      .repartition(8) 

    val props = new Properties()
    props.setProperty("user", jdbcUser)
    props.setProperty("password", jdbcPassword)
    props.setProperty("driver", "org.postgresql.Driver")
    props.setProperty("batchsize", "20000")

    fact.write
      .mode("append")
      .jdbc(jdbcUrl, targetTable, props)

    spark.stop()
  }
}
