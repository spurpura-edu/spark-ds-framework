package tech.cornell.datascience

import org.apache.spark.sql.{SparkSession, _}

/**
  * Simple class to hold S3 connection params.
  */
class S3Context(val S3_KEY: String, val S3_SECRET: String)

object S3Context {
  // Access credentials
  // Get them from set ENV vars, or add default credentials.
  private val S3_KEY = sys.env.getOrElse("S3_KEY_ID", "X")
  private val S3_SECRET = sys.env.getOrElse("S3_KEY_SECRET", "Y")

  // Factory methods, in case connection parameters are to be passed as method parameters
  def apply(S3_KEY: String = S3_KEY, S3_SECRET: String = S3_SECRET): S3Context = new S3Context(S3_KEY, S3_SECRET)
}

object SparkS3Client {
  /**
    * Reads a CSV file from a S3 bucket and loads it into a Dataframe.
    *
    * @param bucket   : bucket name.
    * @param filename : name of the file to read.
    * @param header   : whether the file read includes a header to put in the dataframe.
    * @param ss       : SparkSession object.
    * @param s3c      : S3Context object holding connection params.
    * @return a DataFrame object from the file
    */
  def readCSV(bucket: String, filename: String, header: Boolean = true)
             (implicit ss: SparkSession, s3c: S3Context = S3Context()): DataFrame = {
    ss.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3c.S3_KEY)
    ss.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3c.S3_SECRET)
    ss
      .read
      .option("header", header) //assume there are always headers in our csv files.
      //for simplicity, infer schema. If you have mixed types (float, boolean) on a column, it will default to string
      .option("inferSchema", true)
      .csv(s"s3n://$bucket/$filename")
  }

  /**
    * Writes a Spark DataFrame to an S3 bucket.
    *
    * @param bucket    : bucket name.
    * @param filename  : name of the file to write.
    * @param dataFrame : dataframe to write.
    * @param header    : whether to include the dataframe header in the file.
    * @param ss        : SparkSession object.
    * @param s3c       : S3Context object holding connection params.
    */
  def writeCSV(bucket: String, filename: String, dataFrame: DataFrame, header: Boolean = true)
              (implicit ss: SparkSession, s3c: S3Context = S3Context()) = {
    ss.sparkContext.hadoopConfiguration.set("fs.s3n.awsAccessKeyId", s3c.S3_KEY)
    ss.sparkContext.hadoopConfiguration.set("fs.s3n.awsSecretAccessKey", s3c.S3_SECRET)
    dataFrame
      .coalesce(1) // write a single file
      .write
      .option("header", header) //assume there are always headers in our csv files.
      .csv(s"s3n://$bucket/$filename")
  }
}