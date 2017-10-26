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

    /*val cleanLines = lines.flatMap { line => line.split(',') }
      .filter { !_.isEmpty }
    lines.map()
    */



    val lines = sc.textFile("/Users/jashanrandhawa/Downloads/MillionSongSubset/song_info.csv")
    val headerAndRows = lines.map(line => line.split(";").map(_.trim))
    val header = headerAndRows.first
    val data = headerAndRows.filter(_(0) != header(0))
    getUnique(data);
    getSong(data)





    df.printSchema()
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    log.info(s"Lines with a: $numAs, Lines with b: $numBs")
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }


  def getUnique( x : rdd.RDD[Array[String]] ): Unit ={
    val uniq_artist = x.map { _(16) }.distinct.count
    val uniq_song = x.map { _(23) }.distinct.count
    val uniq_alb = x.map { _(22) }.distinct.count
    println(uniq_artist, uniq_alb, uniq_song)
  }

  def getSong( x : rdd.RDD[Array[String]] ): Unit = {
    var x1 = checkValidity("3", x)
    var x2 = checkValidity("2", x1)
    //val songs = x2.map { _(23) }
    //val songnm = x2.map { _(24) }
    var songtuple = x2.map { _(23) } zip x2.map { _(24) }             //(song_id, sond_name) tuple
    val loud = x2.map { _(6) }
    val top5loud = songtuple.zip(loud).map { x => (x._1, x._2.toDouble) }.collect.toList.sortWith(_._2>_._2).take(5)


    x2 = checkValidity("4", x1)
    songtuple = x2.map { _(23) } zip x2.map { _(24) }                 //(song_id, sond_name) tuple
    val long1 = x2.map { _(5) }
    val top5long = songtuple.zip(long1).map { x => (x._1, x._2.toDouble) }.collect.toList.sortWith(_._2>_._2).take(5)

    x2 = checkValidity("5", x1)
    songtuple = x2.map { _(23) } zip x2.map { _(24) }                  //(song_id, sond_name) tuple
    val tempo = x2.map { _(7) }
    val top5fast = songtuple.zip(tempo).map { x => (x._1, x._2.toDouble) }.collect.toList.sortWith(_._2>_._2).take(5)

    x2 = checkValidity("11", x1)
    songtuple = x2.map { _(23) } zip x2.map { _(24) }                  //(song_id, sond_name) tuple
    val hotns = x2.map { _(25) }
    val top5hot = songtuple.zip(hotns).map { x => (x._1, x._2.toDouble) }.collect.toList.sortWith(_._2>_._2).take(5)

  }

  def getArtist( x : rdd.RDD[Array[String]] ): Unit = {
    var x1 = checkValidity("3", x)

  }

  def getFam(): Unit ={

    val artist = data.map { _(16) }
    val artistnm = data.map { _(17) }

    val artistuple = artistnm zip artist

    artistuple.zip(fam).map { x => x._1, x._2.toDouble }.collect.toList.sortWith(_._2>_._2).take(5)

    val fam = data.map { _(19) }

    var dataFam = data.filter(x => !(x(19).equalsIgnoreCase("na"))).filter(x => x(19).toDouble > 0).


      checkValidity()
  }




  def checkValidity( a : String , data : rdd.RDD[Array[String]] ): rdd.RDD[Array[String]] = {
    a match {
      case "1" => data.filter (x => ! (x(19).equalsIgnoreCase ("na")) )
                    .filter (x => x(19).toDouble > 0)           //Familiarity
      case "2" => data.filter (x => !(x(19).equalsIgnoreCase ("na")) )
                    .filter{_(6).toDouble < 0}                  //Loudness
      case "3" => data.filter (x => ! (x(0).isEmpty) )          //Track_id
      case "4" => data.filter (x => x(5).toDouble > 0)                       //duration
      case "5" => data.filter (x => ! (x(7).equalsIgnoreCase ("na") ) )
                    .filter (x => x(9).toDouble > 0)             //tempo
        /**
      case "6" => var key = data.map {_ (8).toInt}
                    .collect
                    .filter (x => (x >= 0) && (x <= 11) )

      case "7" => var keyConf = data.map {_ (9).toDouble}
                    .collect
                    .filter (x => ! (x.equalsIgnoreCase ("na") ) )
                    .map (x => Try (x.toDouble).getOrElse (0.0) )
                    .filter (x => x > 0)

          */
      case "8" => var artistId = data.map {_ (16)}              //artist_id
                    .collect
                    .filter (x => ! (x.isEmpty) )

      case "9" => var artHot = data.map {_ (20)}                  //artist_name
                    .collect
                    .filter (x => ! (x.equalsIgnoreCase ("na") ) )
                    .map (x => Try (x.toDouble).getOrElse (0.0) )
                    .filter (x => x > 0)
      /**
      case "10" => var title = data.map {_ (24)}
                    .collect
                    .filter (x => ! (x.isEmpty) )
        */
      case "11" =>  data.filter (x => ! (x(25).equalsIgnoreCase ("na") ) )
                      .filter (x => x(25).toDouble >= 0)                                 //hotness
        /**
      case "12" => var year = data.map {_ (26)}
                    .collect
                    .filter (x => ! (x.equalsIgnoreCase ("na") ) )
                    .map (x => Try (x.toInt).getOrElse (0) )
                    .filter (x => x > 0)
        */
    }
  }

}
