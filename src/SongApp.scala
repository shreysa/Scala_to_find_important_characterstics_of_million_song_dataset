/**
 * CS6240 - Fall 2017
 * Assignment A6
 * Spark and Scala test program 
 *
 * @author Shreysa Sharma and Jashangeet Singh
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j.{LogManager, Level, PropertyConfigurator}
import org.apache.spark.rdd
import java.util.Calendar

object SongApp {

  var startTime = Calendar.getInstance().getTime().getMinutes
  def main(args: Array[String]) {
    
    // Set-up logging
    // Logging code derived from
    // https://syshell.net/2015/02/03/spark-configure-and-use-log4j/
    PropertyConfigurator.configure("./src/log4j.properties")
    val rootLog = LogManager.getRootLogger()
    rootLog.setLevel(Level.OFF)

 
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)

    //val lines = sc.textFile("/Users/jashanrandhawa/Downloads/spark-2.2.0-bin-hadoop2.7/a6/a61/all/song_info.csv")
    //val lines = sc.textFile("/Users/shreysa/Academics/CS6240/A6/a6-jashangeet-shreysa/data/CompleteDataset/song_info.csv")
    val lines = sc.textFile("/Users/shreysa/Academics/CS6240/A6/a6-jashangeet-shreysa/data/MillionSongSubset/song_info.csv")
    val headerAndRows = lines.map(line => line.split(";").map(_.trim))
    val header = headerAndRows.first
    val data = headerAndRows.filter(_(0) != header(0))
    getUnique(data)
    getTopSong(data)
    getTopArtist(data)
    getTopKey(data, sc)
    getTopWords(data, sc)
    getTopGenre(data, sc)
  }


  def getUnique( x : rdd.RDD[Array[String]] ): Unit ={
    val uniq_artist = x.map { _(16) }.distinct.count
    val uniq_song = x.map { _(23) }.distinct.count
    val uniq_alb = x.map { _(22) }.distinct.count
    println("Count of distinct artists "  + uniq_artist)
    println("Count of distinct albums "  + uniq_alb)
    println("Count of distinct songs "  + uniq_song)
  
  }

  def getTopSong( x : rdd.RDD[Array[String]] ): Unit = {
    var x1 = checkValidity("3", x)
    var x2 = checkValidity("2", x1)
   
    var songtuple = x2.map { _(23) } zip x2.map { _(24) }             //(song_id, song_name) tuple
    val loud = x2.map { _(6) }
    val top5loud = songtuple.zip(loud).map { x => (x._1, x._2.toDouble) }.sortBy(_._2, false).take(5).toList
	println("The top 5 loud songs are: " + top5loud)


    x2 = checkValidity("4", x1)
    songtuple = x2.map { _(23) } zip x2.map { _(24) }                 //(song_id, song_name) tuple
    val long1 = x2.map { _(5) }
    val top5long = songtuple.zip(long1).map { x => (x._1, x._2.toDouble) }.sortBy(_._2, false).take(5).toList
    println("The top 5 long songs are: " + top5long)

    x2 = checkValidity("5", x1)
    songtuple = x2.map { _(23) } zip x2.map { _(24) }                  //(song_id, song_name) tuple
    val tempo = x2.map { _(7) }
    val top5fast = songtuple.zip(tempo).map { x => (x._1, x._2.toDouble) }.sortBy(_._2, false).take(5).toList
    println("The top 5 fast songs are: " + top5fast)

    x2 = checkValidity("11", x1)
    songtuple = x2.map { _(23) } zip x2.map { _(24) }                  //(song_id, song_name) tuple
    val hotns = x2.map { _(25) }
    val top5hot = songtuple.zip(hotns).map { x => (x._1, x._2.toDouble) }.sortBy(_._2, false).distinct.take(5).toList
    println("The top 5 hot songs are: " + top5hot)
    
  }

  def getTopArtist( x : rdd.RDD[Array[String]] ): Unit = {
    var x1 = checkValidity("3", x)
    var x2 = checkValidity("8", x1)

    var artistuple = x2.map{ _(16) } zip x2.map{ _(17) }
    val top5Art = artistuple.mapValues(x => (x,1)).reduceByKey((x, y) => (x._1, x._2 + y._2)).sortBy(_._2._2, false).take(5).toList
    println("The top 5 prolific artists are: " + top5Art)

    var x3 = checkValidity("1", x2)
    artistuple = x3.map { _(17) } zip x3.map { _(16) }                //(artist_name, artist_id) tuple
    val fam = x3.map { _(19) }
    val top5famArt = artistuple.zip(fam).map { x => (x._1, x._2.toDouble) }.distinct.sortBy(_._2, false).take(5).toList
    println("The top 5 familiar artists are: " + top5famArt)

    x3 = checkValidity("9", x2)
    artistuple = x3.map { _(17) } zip x3.map { _(16) }                //(artist_name, artist_id) tuple
    val artHot = x3.map { _(20) }
    val top5famArtHot = artistuple.zip(artHot).map { x => (x._1, x._2.toDouble) }.distinct.sortBy(_._2, false).take(5).toList
    println("The top 5 hot artists are: " + top5famArtHot)
  }

  def getTopKey( x : rdd.RDD[Array[String]], sc : SparkContext ): Unit ={
    var x1 = checkValidity("6", x)
    var x2 = checkValidity("9", x1)
    val top5key = sc.parallelize(x2.map{ _(8) }.countByValue.toSeq).sortBy(_._2, false).take(5).toList
    println("The top 5 keys are: " + top5key)
  }


  def getTopWords( x : rdd.RDD[Array[String]], sc : SparkContext): Unit ={
    var x1 = checkValidity("10", x)
    val words = x1.map{_(24)}.flatMap { line => line.filter(c => c.isLetter || c.isWhitespace).toUpperCase.split(' ') }.filter { !_.isEmpty }
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
    val artist_hotness = x.map(lines => (lines(16), lines(20)))
    //val artist_dist_hot = artist_hotness.distinct.map(lines => lines)
    val art_coll = artist_hotness.distinct.collect
    val top5Genres = artist_term.map(lines => (lines._1, art_coll.filter(_._1 == lines._2)(0)._2))
    .mapValues(x => (x.toDouble, 1))
    .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
    .mapValues(y => 1.0 * y._1 / y._2).sortBy(_._2, false).take(5).toList
    println("The top 5 genres are : " + top5Genres)

  }


  def checkValidity( a : String , data : rdd.RDD[Array[String]] ): rdd.RDD[Array[String]] = {
    a match {
      case "1" => data.filter (x => ! (x(19).equalsIgnoreCase ("na")) )
                    .filter (x => x(19).toDouble > 0)                     //Familiarity
      case "2" => data.filter (x => !(x(19).equalsIgnoreCase ("na")) )
                    .filter{_(6).toDouble < 0}                              //Loudness
      case "3" => data.filter (x => ! (x(0).isEmpty) )                      //Track_id
      case "4" => data.filter (x => x(5).toDouble > 0)                       //duration
      case "5" => data.filter (x => ! (x(7).equalsIgnoreCase ("na") ) )
                    .filter (x => x(9).toDouble > 0)                         //tempo

      case "6" => data.filter (x => (x(8).toInt >= 0) && (x(8).toInt <= 11) )       //key

      case "7" => data.filter (x => ! (x(9).equalsIgnoreCase ("na") ) )         //key_conf
                    .filter (x => x(9).toDouble > 0.7)

      case "8" => data.filter (x => ! (x(16).isEmpty))                                    //artist_id

      case "9" => data.filter (x => ! (x(20).equalsIgnoreCase ("na")))
                    .filter (x => (x(20).toDouble >= 0) && (x(20).toDouble <= 1))                //artist_hotness

      case "10" => data.filter (x => ! (x(24).isEmpty) )                    //title
      case "11" => data.filter (x => ! (x(25).equalsIgnoreCase ("na") ) )
                      .filter (x => (x(25).toDouble >= 0) && (x(25).toDouble <= 1))                //song_hotness
       
    }
  }
}
