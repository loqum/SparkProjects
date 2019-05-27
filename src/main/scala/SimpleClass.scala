import org.apache.spark.{SparkConf, SparkContext}

class SimpleClass {

  def simple(): Unit = {
    val logFile = "lorem.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]").set("spark.executor.memory", "1g")

    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache
    val numAs = logData.filter(line => line.contains("Lorem")).count
    val numBs = logData.filter(line => line.contains("sit")).count
    println("Lines with Lorem: %s, Lines with sit: %s".format(numAs, numBs))
    System.in.read
  }

}
