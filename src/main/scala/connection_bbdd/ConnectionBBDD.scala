package connection_bbdd

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

class ConnectionBBDD {

  def connectionToMySql(): Unit = {

    //val query = "CREATE TABLE `sample`.`spaniards` (`id` INT NOT NULL,`name` VARCHAR(45) NULL,`surname` VARCHAR(45) NULL,`city` VARCHAR(45) NULL,PRIMARY KEY (`id`))"
    val dbtable = selectBD()
    val url = "jdbc:mysql://localhost:3306/sample?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC"

    val sc = SparkSession
      .builder
      .appName("Spark SQL basic example")
      .master("local")
      .enableHiveSupport
      .getOrCreate

    val dataframe = sc
      .read
      .format("jdbc")
      .option("url", url)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", dbtable)
      .option("user", "root")
      .option("password", "root")
      .load


    // Looks the schema of this DataFrame.
    dataframe.printSchema
    dataframe.show

    // Counts people by age
    val countsByAge = dataframe.groupBy("gender").count()
    countsByAge.show

    print(dataframe)
    //case class Client(name: String, zip:Option[Int])

    import sc.sqlContext.implicits._

    val dfSpain = dataframe.select("name", "surname", "city").filter("country_code == 'ES'")
    val geo = dataframe.select($"geo_dis").map(_.getDouble(0))

    val result = geo.collect

    val rows: RDD[Row] = dataframe.rdd

    val row = sc.sparkContext.parallelize(result)

    val reduceSum = row.reduce { (x, y) => x + y }
    print(reduceSum)

    //dataframe.as[Client].map(x => x)

    // Saves countsByAge to S3 in the JSON format.
    dfSpain.write.mode(SaveMode.Overwrite).format("json").save("ES")
    geo.write.mode(SaveMode.Overwrite).format("json").save("GEO_DIS")
    dataframe.write.mode(SaveMode.Overwrite).format("json").save("outJSON")
    dataframe.write.mode(SaveMode.Overwrite).format("csv").save("outCSV")

    val prop = new java.util.Properties
    prop.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")


    /*val foo = Seq(
      (dataframe.select("name").map(_.getString(0)).collect().deep.mkString(", "), "names"),
      (dataframe.select("surname").map(_.getString(0)).collect().deep.mkString(", "), "names"),
      (dataframe.select("country_code").map(_.getString(0)).collect().deep.mkString(", "), "country_code")
    ).toDF("names", "surname", "country_code")*/

    sc.sql("select * from clients").show

    /*val name = dataframe.select("name").map(_.getString(0)).collect()
    val surname = dataframe.select("surname").map(_.getString(0)).collect()
    val country_code = dataframe.select("country_code").map(_.getString(0)).collect()

    val foo = Seq((
      (name, "name"),
      (surname, "surname"),
      (country_code, "country_code")
    )).toDF("names", "surname", "country_code")

    foo.write
      .format("jdbc")
      .option("url", url)
      .option("driver", "com.mysql.cj.jdbc.Driver")
      .option("dbtable", "spaniards")
      .option("delimiter", "\\u0001")
      .option("user", "root")
      .option("password", "root")
      .save()*/

  }

  private def selectBD(): String = {
    println("Dime cómo se llama la tabla, sabrosura")

    val scanner = new java.util.Scanner(System.in)

    val dbtable = scanner.nextLine()

    dbtable
  }

}
