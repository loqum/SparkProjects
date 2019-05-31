package word_count

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

class WordCountOld {

  def count(): Unit = {

    val conf = new SparkConf().setAppName("AssociationRulesExample").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = SparkSession
      .builder
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate

    val linesDF = sc.read.textFile("lorem.txt").toDF("line")
    val wordsDF = linesDF.explode("line", "word")((line: String) => line.split(" "))

    val wordCountDF = wordsDF.groupBy("word").count
    wordCountDF.show
    wordCountDF.coalesce(1).write.json("resultado")

    /*val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)


    val juntito = counts.collect
    juntito.saveAsTextFile("loremOut")
    counts.collect.foreach(println)*/

  }

}
