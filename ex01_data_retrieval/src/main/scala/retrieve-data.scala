import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import java.net.URL
import java.nio.file.{ Files, Paths, StandardCopyOption }

object Download {

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      System.err.println(
        "Usage : sbt \"run <parquet-url>\""
      )
      sys.exit(1)
    }

    val parquetUrl = args(0)

    // merci gpt pour cette immonde regex
    val Pattern = """.*_(\d{4})-(\d{2})\.parquet""".r

    val (year, month) = parquetUrl match {
      case Pattern(y, m) =>
        (y, m)

      case _ =>
        System.err.println("Impossible d'extraire year/month depuis l'URL")
        sys.exit(1)
    }

    val localPath =
      s"/tmp/yellow_tripdata_$year-$month.parquet"

    println(s"Téléchargement depuis $parquetUrl")

    val in = new URL(parquetUrl).openStream()
    Files.copy(
      in,
      Paths.get(localPath),
      StandardCopyOption.REPLACE_EXISTING
    )
    in.close()

    println(s"Fichier téléchargé vers $localPath")

    val spark = SparkSession
      .builder()
      .appName("NYC Taxi - Download")
      .master("local[*]")
      .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
      .config("spark.hadoop.fs.s3a.access.key", "minio")
      .config("spark.hadoop.fs.s3a.secret.key", "minio123")
      .config("spark.hadoop.fs.s3a.path.style.access", "true")
      .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
      .getOrCreate()

    val df = spark.read.parquet(localPath)

    val outputPath =
      s"s3a://nyc-raw/yellow_tripdata/year=$year/month=$month"

    df.write
      .mode("append")
      .parquet(outputPath)

    println(s"Données écrites dans MinIO : $outputPath")

    val test = spark.read.parquet(outputPath)
    test.printSchema()
    test.show(5, false)

    spark.stop()
  }
}
