package mllib_basics

import java.util.Properties

import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SparkSession

class KMeansMySqlOther {

  def kMeansMySqlOther: Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark SQL basic example")
      .master("local")
      .getOrCreate

    val url = "jdbc:mysql://localhost:3306/sample?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"
    val table = "clients"
    val properties = new Properties
    properties.put("user", "root")
    properties.put("password", "root")

    //val clientsTable = spark.read.jdbc(url, table, properties)

    val dataset = spark.read.option("header", "true").option("inferSchema", "true").jdbc(url, table, properties)

    // Select the following columns for the training set:
    // Fresh, Milk, Grocery, Frozen, Detergents_Paper, Delicassen
    // Cal this new subset feature_data

    import spark.sqlContext.implicits._

    val feature_data = dataset.select($"geo_dis", $"exp_dis")

    val assembler = new VectorAssembler().setInputCols(Array("geo_dis", "exp_dis")).setOutputCol("features")

    val training_data = assembler.transform(feature_data).select("features")

    val kmeans = new KMeans().setK(3).setSeed(1L)

    val model = kmeans.fit(training_data)

    val WSSSE = model.computeCost(training_data)
    println(s"Within Set Sum of Squared Errors = $WSSSE")

    println("Cluster Centers: ")
    model.clusterCenters.foreach(println)

    System.in.read
  }

}
