package sample

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by micchon1 on 2016/08/08.
  */
object BasicWordCount {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("Basic WordCount")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("./data/README.md")
    val words = textFile.flatMap(line => line.split(" "))
    val wordCounts = words.map(word => (word, 1)).reduceByKey((a, b) => a + b)

    wordCounts.saveAsTextFile("./wordcounts")
  }
}
