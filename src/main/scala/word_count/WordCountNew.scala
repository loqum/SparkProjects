package word_count

import org.apache.spark.sql.{SaveMode, SparkSession}

class WordCountNew {

  def run: Unit = {
    val inputPath = "C:\\Users\\ruben\\Desktop\\simulation.txt"
    val outputPath = "C:\\Users\\ruben\\Desktop\\result_simulation"
    val url = "jdbc:mysql://localhost:3306/sample?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val dbtable = "wordcount_simulation"
    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    val sc = SparkSession
      .builder
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate

    import sc.sqlContext.implicits._

    val linesRDD = sc.read.textFile(inputPath).rdd

    val wordsRDD = linesRDD.flatMap(_.split(" ")).map(word => (word.toLowerCase, 1)).reduceByKey(_ + _)

    val wordsDF = wordsRDD.toDF("word", "total")

    wordsDF
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .json(outputPath)

    val tsvWithHeaderOptions: Map[String, String] = Map(
      ("delimiter", "\t"),
      ("header", "true"))

    wordsDF
      .select("word", "total")
      .coalesce(1)
      .write
      .options(tsvWithHeaderOptions)
      .mode(SaveMode.Append)
      .csv(outputPath)

    wordsDF.write.mode("overwrite").jdbc(url, dbtable, prop)
  }

}