/**
 * CS6240 - Fall 2017
 * Assignment A6
 * Spark and Scala
 *
 * @author Shreysa Sharma
 */

import org.apache.log4j.{LogManager, Level, PropertyConfigurator}
import org.apache.spark.rdd

class CleanUp(row : String) extends java.io.Serializable {
    
    val line : Array[String] = row.split(";")
    var trackId = ""
    var duration = 0.00
    var loudness = 0.00
    var tempo = 0.00
    var key = 0
    var keyConf = 0.00
    var artistId = ""
    var artFam = 0.00
    var artHot = 0.00
    var songHot = 0.00
    var title = ""
    var isValidRow = true;

    // Local logger
    val log = LogManager.getLogger("CleanUp")
    log.setLevel(Level.INFO)

    // Converting values to respective data types and eliminating invalid record
    try {
      duration = line(5).toDouble
      loudness = line(6).toDouble
      tempo = line(7).toDouble
      key = line(8).toInt
      keyConf = line(9).toDouble
      artFam = line(19).toDouble
      artHot = line(20).toDouble
      songHot = line(25).toDouble
    } catch {
      case e: Exception => isValidRow = false
    }

    
    def getKey()      : Int = key
    def getDuration() : Double = duration
    def getLoudness() : Double = loudness
    def getTempo()    : Double = tempo
    def getKeyConf()  : Double = keyConf
    def getArtFam()   : Double = artFam
    def getArtHot()   : Double = artHot
    def getSongHot()  : Double = songHot
    def getArtId()    : String = line(16)
    def getAlbum()    : String = line(22)
    def getSongId()   : String = line(23)
    def getTitle()    : String = line(24)

      
    // def getTopSong( x : rdd.RDD[Array[String]] ): Unit = {
    //   var x1 = checkValidity("3", x)
    //   var x2 = checkValidity("2", x1)
    
    //   var songtuple = x2.map { _(23) } zip x2.map { _(24) }             //(song_id, song_name) tuple
    //   val loud = x2.map { _(6) }
    //   val top5loud = songtuple.zip(loud).map { x => (x._1, x._2.toDouble) }.collect.toList.sortWith(_._2>_._2).take(5)
    //   println("The top 5 loud songs are: " + top5loud)


    //   x2 = checkValidity("4", x1)
    //   songtuple = x2.map { _(23) } zip x2.map { _(24) }                 //(song_id, song_name) tuple
    //   val long1 = x2.map { _(5) }
    //   val top5long = songtuple.zip(long1).map { x => (x._1, x._2.toDouble) }.collect.toList.sortWith(_._2>_._2).take(5)
    //   println("The top 5 long songs are: " + top5long)

    //   x2 = checkValidity("5", x1)
    //   songtuple = x2.map { _(23) } zip x2.map { _(24) }                  //(song_id, song_name) tuple
    //   val tempo = x2.map { _(7) }
    //   val top5fast = songtuple.zip(tempo).map { x => (x._1, x._2.toDouble) }.collect.toList.sortWith(_._2>_._2).take(5)
    //   println("The top 5 fast songs are: " + top5fast)

    //   x2 = checkValidity("11", x1)
    //   songtuple = x2.map { _(23) } zip x2.map { _(24) }                  //(song_id, song_name) tuple
    //   val hotns = x2.map { _(25) }
    //   val top5hot = songtuple.zip(hotns).map { x => (x._1, x._2.toDouble) }.collect.toList.sortWith(_._2>_._2).distinct.take(5)
    //   println("The top 5 hot songs are: " + top5hot)
      
    // }


    def checkValidity() : Boolean = {
      var result = false
     if(isValidRow && getDuration() > 0.00
       && getLoudness() < 0.00
       && getTempo() > 0.00
       && getKey() > 0
       && getKeyConf() > 0 && getKeyConf() < 1
       && getArtFam() > 0
       && getArtHot() > 0
       && getSongHot() > 0
       && !(getAlbum.equalsIgnoreCase ("na"))
       && !(getTitle().isEmpty) && !(getSongId().isEmpty) && !(getArtId().isEmpty)) {
       result = true
     }
      result
     }

    //  def checkValidity( a : String , data : rdd.RDD[Array[String]] ): rdd.RDD[Array[String]] = {
    // a match {
    //   case "1" => data.filter (x => ! (x(19).equalsIgnoreCase ("na")) )
    //                 .filter (x => x(19).toDouble > 0)                     //Familiarity
    //   case "2" => data.filter (x => !(x(19).equalsIgnoreCase ("na")) )
    //                 .filter{_(6).toDouble < 0}                              //Loudness
    //   case "3" => data.filter (x => ! (x(0).isEmpty) )                      //Track_id
    //   case "4" => data.filter (x => x(5).toDouble > 0)                       //duration
    //   case "5" => data.filter (x => ! (x(7).equalsIgnoreCase ("na") ) )
    //                 .filter (x => x(9).toDouble > 0)                         //tempo

    //   case "6" => data.filter (x => (x(8).toInt >= 0) && (x(8).toInt <= 11) )       //key

    //   case "7" => data.filter (x => ! (x(9).equalsIgnoreCase ("na") ) )         //key_conf
    //                 .filter (x => x(9).toDouble > 0.7)

    //   case "8" => data.filter (x => ! (x(16).isEmpty))                                    //artist_id

    //   case "9" => data.filter (x => ! (x(20).equalsIgnoreCase ("na")))
    //                 .filter (x => (x(20).toDouble >= 0) && (x(20).toDouble <= 1))                //artist_hotness

    //   case "10" => data.filter (x => ! (x(24).isEmpty) )                    //title
    //   case "11" => data.filter (x => ! (x(25).equalsIgnoreCase ("na") ) )
    //                   .filter (x => (x(25).toDouble >= 0) && (x(25).toDouble <= 1))                //song_hotness
       
    // }

}
