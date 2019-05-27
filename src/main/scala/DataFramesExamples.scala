import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

class DataFramesExamples {

  def textSearch(): Unit = {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = SparkSession
      .builder
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate

    val textFile = sc.read.textFile("lorem.txt")
    val dataFrame = textFile.toDF("line")
    val lorem = dataFrame.filter("line")


  }

}
