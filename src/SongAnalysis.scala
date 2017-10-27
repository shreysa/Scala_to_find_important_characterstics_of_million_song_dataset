/**
  * CS6240 - Fall 2017
  * Assignment A6
  * Spark and Scala test program
  *
  * @author Shreysa Sharma and Jashangeet Singh
  */

import org.apache.log4j.{LogManager, Level, PropertyConfigurator}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd
import java.util.Calendar

object SongAnalysis {
  var startTime = Calendar.getInstance().getTime().getMinutes

  def main(args: Array[String]) {
    val inputFileName = args(0)

    // Set-up logging
    // Logging code derived from
    // https://syshell.net/2015/02/03/spark-configure-and-use-log4j/
    PropertyConfigurator.configure("./src/log4j.properties")
    val rootLog = LogManager.getRootLogger()
    rootLog.setLevel(Level.OFF)


    // Local logger
    val log = LogManager.getLogger("SongAnalysis")
    log.setLevel(Level.INFO)
    log.info("Starting SongAnalysis with input file: " + inputFileName)

    // Spark Configuration
    val conf = new SparkConf().setAppName("Song Analysis").setMaster("local")
    val sc = new SparkContext(conf)

    // Start Processing
    val songFile = sc.textFile(inputFileName, 2)
    var records = songFile.map(row => new CleanUp(row))

    // Clean-up
    records = records.filter(_.checkValidity())

    // Task 1
    val uniq_artist = records.map(_.getArtId()).distinct.count
    val uniq_song = records.map(_.getSongId()).distinct.count
    val uniq_alb = records.map(_.getAlbum()).distinct.count
    log.info("[1.a] Count of distinct artists "  + uniq_artist)
    log.info("[1.b] Count of distinct albums "  + uniq_alb)
    log.info("[1.c] Count of distinct songs "  + uniq_song)

    // Task 2 


  }
}
