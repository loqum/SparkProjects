import java.util.Properties

import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
object KMeansMySQL {

  def main(args: Array[String]) {
    val sc = SparkSession
      .builder
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate

    val conf = new SparkConf().setAppName("KMeansExample").set("spark.driver.allowMultipleContexts", "true").setMaster("local")
    val scontext = new SparkContext(conf)
    val url = "jdbc:mysql://localhost:3306/sample?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val table = "clients"
    val properties = new Properties
    properties.put("user", "root")
    properties.put("password", "root")

    val driverClass = "com.mysql.cj.jdbc.Driver"
    // properties.setProperty("Driver", driverClass)
    Class.forName("com.mysql.cj.jdbc.Driver")
    val clientsTable = sc.read.jdbc(url, table, properties)

    val parsedData = clientsTable.rdd.map(_.getAs[Vector]("exp_dis"))

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    // Save and load model
    clusters.save(scontext, "target/org/apache/spark/KMeansExample/KMeansModel")
    val sameModel = KMeansModel.load(scontext, "target/org/apache/spark/KMeansExample/KMeansModel")
    // $example off$

    sc.stop
  }
}
