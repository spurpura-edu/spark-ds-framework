package cornell.datascience.utils

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by moises on 4/21/17.
  */
object SparkFileClient {
  /**
    * Reads a CSV file from a S3 bucket and loads it into a Dataframe.
    */
  def readTSV(bucket: String, filename: String, header: Seq[String])
             (implicit ss: SparkSession, s3c: S3Context = S3Context()): DataFrame = {
    ss.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3c.S3_KEY)
    ss.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3c.S3_SECRET)
    ss
      .read
      .option("header", true) //assume there are always headers in our csv files.
      .option("inferSchema", true) //always try to infer the schema.
      .csv(s"s3n://$bucket/$filename")
      .toDF(header: _*)
  }

  def writeTSV(bucket: String, filename: String, dataFrame: DataFrame)
              (implicit ss: SparkSession, s3c: S3Context = S3Context()) = {
    ss.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3c.S3_KEY)
    ss.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3c.S3_SECRET)
    dataFrame
      .coalesce(1)
      .write
      .option("header", true) //assume there are always headers in our csv files.
      .csv(s"s3n://$bucket/$filename")
  }

  /**
    * Reads a CSV file from a S3 bucket and loads it into a Dataframe.
    */
  def readTSV(filename: String, header: Seq[String])(implicit ss: SparkSession): DataFrame = {
    ss
      .read
      .option("header", false) //assume there are always headers in our csv files.
      .option("inferSchema", true) //always try to infer the schema.
      .option("delimiter", "\t")
      .csv(filename)
      .toDF(header: _*)
  }

  def writeTSV(filename: String, dataFrame: DataFrame)
              (implicit ss: SparkSession) = {
    dataFrame
      .coalesce(1)
      .write
      .option("header", true) //assume there are always headers in our csv files.
      .csv(filename)
  }

}
