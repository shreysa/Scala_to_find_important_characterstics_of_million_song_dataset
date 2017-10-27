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
    getUnique(records)

    // Task 2
    getTopSong(records)

    //Task 3
    getTopArtist(records)

    //Task 4
    getTopKey(records, sc)

    //Task 5
    getTopWords(records, sc)

    //Task 6
    getTopGenre(records, sc)

  }

  def getUnique( records : rdd.RDD[CleanUp] ): Unit ={
    val uniq_artist = records.map(_.getArtId()).distinct.count
    val uniq_song = records.map(_.getSongId()).distinct.count
    val uniq_alb = records.map(_.getAlbum()).distinct.count
    log.info("[1.a] Count of distinct artists "  + uniq_artist)
    log.info("[1.b] Count of distinct albums "  + uniq_alb)
    log.info("[1.c] Count of distinct songs "  + uniq_song)
  }

  def getTopSong( x : rdd.RDD[CleanUp] ): Unit = {

    var songtuple = x.map { l => (l.getSongId(), l.getTitle()) }             //(song_id, song_name) tuple
    val loud = x.map { _.getLoudness() }
    val top5loud = songtuple.zip(loud).map { x => (x._1, x._2) }.sortBy(_._2, false).take(5).toList
    println("The top 5 loud songs are: " + top5loud)

    val long1 = x.map { _.getDuration() }
    val top5long = songtuple.zip(long1).map { x => (x._1, x._2) }.sortBy(_._2, false).take(5).toList
    println("The top 5 long songs are: " + top5long)

    val tempo = x.map { _.getTempo() }
    val top5fast = songtuple.zip(tempo).map { x => (x._1, x._2) }.sortBy(_._2, false).take(5).toList
    println("The top 5 fast songs are: " + top5fast)

    val hotns = x.map { _.getSongHot() }
    val top5hot = songtuple.zip(hotns).map { x => (x._1, x._2) }.sortBy(_._2, false).distinct.take(5).toList
    println("The top 5 hot songs are: " + top5hot)

  }

  def getTopArtist( x : rdd.RDD[CleanUp] ): Unit = {

    var artistuple = x.map{ l => (l.getArtId(), l.getArtistName()) }
    val top5Art = artistuple.mapValues(x => (x,1)).reduceByKey((x, y) => (x._1, x._2 + y._2)).sortBy(_._2._2, false).take(5).toList
    println("The top 5 prolific artists are: " + top5Art)

    artistuple = x.map { l => (l.getArtistName(), l.getArtId()) }               //(artist_name, artist_id) tuple
    val fam = x.map { _.getArtFam() }
    val top5famArt = artistuple.zip(fam).map { x => (x._1, x._2) }.distinct.sortBy(_._2, false).take(5).toList
    println("The top 5 familiar artists are: " + top5famArt)

    val artHot = x.map { _.getArtHot() }
    val top5famArtHot = artistuple.zip(artHot).map { x => (x._1, x._2) }.distinct.sortBy(_._2, false).take(5).toList
    println("The top 5 hot artists are: " + top5famArtHot)
  }

  def getTopKey( x : rdd.RDD[CleanUp], sc : SparkContext ): Unit ={
    val top5key = sc.parallelize(x.map{ _.getKey() }.countByValue.toSeq).sortBy(_._2, false).take(5).toList
    println("The top 5 keys are: " + top5key)
  }

  def getTopWords( x : rdd.RDD[CleanUp], sc : SparkContext): Unit ={
    val words = x1.map{_.getTitle()}.flatMap { line => line.filter(c => c.isLetter || c.isWhitespace).toUpperCase.split(' ') }.filter { !_.isEmpty }
    val ignored = Set(
      "THAT", "WITH", "THE", "AND", "TO", "OF",
      "A", "IT", "SHE", "HE", "YOU", "IN", "I",
      "HER", "AT", "AS", "ON", "THIS", "FOR",
      "BUT", "NOT", "OR", "ME", "MY", "LP", "VERSION", "IS", "ALBUM", "AN", "THE")
    val top5words = sc.parallelize(words.filter { !ignored.contains(_) }.countByValue.toSeq).sortBy(_._2, false).take(5).toList

    println("the top 5 words are: " + top5words)
    val endTime = Calendar.getInstance().getTime().getMinutes - startTime
    println("Total Time taken: " + endTime)
  }

  def getTopGenre( x : rdd.RDD[Array[String]], sc : SparkContext): Unit ={
    val lines = sc.textFile("/Users/shreysa/Academics/CS6240/A6/a6-jashangeet-shreysa/data/MillionSongSubset/artist_terms.csv")
    val headerAndRows = lines.map(line => line.split(";").map(_.trim))
    val header = headerAndRows.first
    val data1 = headerAndRows.filter(_(0) != header(0))

    val artist_term = data1.map(lines => (lines(1), lines(0)))
    val artist_hotness = x.map(lines => (lines.getArtId(), lines.getArtHot))
    //val artist_dist_hot = artist_hotness.distinct.map(lines => lines)
    val art_coll = artist_hotness.distinct.collect
    val top5Genres = artist_term.map(lines => (lines._1, art_coll.filter(_._1 == lines._2)(0)._2))
      .mapValues(x => (x, 1))
      .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
      .mapValues(y => 1.0 * y._1 / y._2).sortBy(_._2, false).take(5).toList
    println("The top 5 genres are : " + top5Genres)

  }



}
