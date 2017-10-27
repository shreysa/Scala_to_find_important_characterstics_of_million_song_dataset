/**
  * CS6240 - Fall 2017
  * Assignment A6
  * Spark and Scala test program
  *
  * @author Shreysa Sharma and Jashangeet Singh
  */
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd
import java.util.Calendar

object SongAnalysis {
  var startTime = Calendar.getInstance().getTime().getMinutes
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Song Analysis").setMaster("local")
    val sc = new SparkContext(conf)

    val songFile = sc.textFile("./data/CompleteDataset/song_info.csv", 2)
    // val songFile = sc.textFile("./data/MillionSongSubset/song_info.csv", 2)

    var records = songFile.map(row => new CleanUp(row))
    var cleanedRec = records.filter(_.checkValidity())

  }
}