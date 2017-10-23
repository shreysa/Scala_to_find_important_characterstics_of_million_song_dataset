/**
 * CS6240 - Fall 2017
 * Assignment A6
 * Spark and Scala test program 
 *
 * @author Shreysa Sharma
 */

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{LogManager, Level, PropertyConfigurator}

object SimpleApp {
  def main(args: Array[String]) {
    
    // Set-up logging
    // Logging code derived from
    // https://syshell.net/2015/02/03/spark-configure-and-use-log4j/
    PropertyConfigurator.configure("./src/log4j.properties")
    val rootLog = LogManager.getRootLogger()
    rootLog.setLevel(Level.WARN)

    // Local logger
    val log = LogManager.getLogger("SimpleApp")
    log.setLevel(Level.INFO)
    log.info("Starting...")

    val logFile = "./data/input.txt"
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)
    val df = sqlContext.read
      .format("com.databricks.spark.csv")
      .option("header", "true")        // Use first line of all files as header
      .option("delimiter", ";")        // Use ';' as delimiter
      .option("inferSchema", "true")   // Automatically infer data types
      .option("mode", "DROPMALFORMED") // DROPS lines that are malformed. Other options PERMISSIVE | FAILFAST
      .load("./data/song_info.csv")

    df.printSchema()

    val selectedData = df.select("artist_id", "artist_name")
    selectedData.write
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .save("./data/output")

    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    log.info(s"Lines with a: $numAs, Lines with b: $numBs")
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
