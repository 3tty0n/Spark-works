package sample

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by micchon1 on 2016/08/08.
  */
object BasicFilter {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Basic Filter")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("./data/README.md")
    val filteredLine = textFile.filter(line => line.contains("Scala"))

    filteredLine.saveAsTextFile("./filter")
  }
}
