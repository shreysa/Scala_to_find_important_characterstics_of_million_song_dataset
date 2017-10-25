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

    val cleanLines = lines.flatMap { line => line.split(',') }
      .filter { !_.isEmpty }
    lines.map()

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

  def filter(): Unit ={

  }

  def getUnique(): Unit = {
    val lines = sc.textFile("/Users/jashanrandhawa/Downloads/MillionSongSubset/song_info.csv")
    val headerAndRows = lines.map(line => line.split(";").map(_.trim))
    val header = headerAndRows.first
    val data = headerAndRows.filter(_(0) != header(0))
    //val maps = data.map(splits => header.zip(splits).toMap)
    //val dataArray = data.map { _(0).split(";") }
    dataArray.map { _(16) }.distinct.count
    dataArray.map { _(0) }.distinct.count
    dataArray.map { _() }.distinct.count
  }

  def loudness(): Unit = {
    val top5 = data.map { _(6).toDouble }
      .collect
      .toList
      .sortWith(_ > _)
      .take(5)
  }
}
