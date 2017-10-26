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

  def CheckValidity(): Unit = {
    var loud = data.map{_(6).toDouble} 
    .filter(x => x < 0) 
    .collect 

    var trackId = data.map{_(0)} 
    .collect 
    .filter(x => !(x.isEmpty)) 

    var duration = data.map{_(5).toDouble} 
    .collect 
    .filter(x => x > 0) 

    var tempo = data.map{_(7).toDouble} 
    .collect
    .filter(x => !(x.equalsIgnoreCase("na"))) 
    .map(x => Try(x.toDouble).getOrElse(0.0)) 
    .filter(x => x > 0)

    var key = data.map{_(8).toInt} 
    .collect 
    .filter(x => (x >= 0) && (x <= 11))

    var keyConf = data.map{_(9).toDouble} 
    .collect 
    .filter(x => !(x.equalsIgnoreCase("na"))) 
    .map(x => Try(x.toDouble).getOrElse(0.0)) 
    .filter(x => x > 0)

    var artistId = data.map{_(16)} 
    .collect 
    .filter(x => !(x.isEmpty))

    var artFam = data.map{_(19)}
    .collect
    .filter(x => !(x.equalsIgnoreCase("na"))) 
    .map(x => Try(x.toDouble).getOrElse(0.0)) 
    .filter(x => x > 0)

    var artHot = data.map{_(20)}
    .collect
    .filter(x => !(x.equalsIgnoreCase("na"))) 
    .map(x => Try(x.toDouble).getOrElse(0.0)) 
    .filter(x => x > 0)

    var title = data.map{_(24)} 
    .collect 
    .filter(x => !(x.isEmpty))

    var songHot = data.map{_(25)} 
    .collect 
    .filter(x => !(x.equalsIgnoreCase("na"))) 
    .map(x => Try(x.toDouble).getOrElse(0.0)) 
    .filter(x => x > 0)

    var year = data.map{_(26)}
    .collect 
    .filter(x => !(x.equalsIgnoreCase("na"))) 
    .map(x => Try(x.toInt).getOrElse(0)) 
    .filter(x => x > 0)


  }

  def
}
