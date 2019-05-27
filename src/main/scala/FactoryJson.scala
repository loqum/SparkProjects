import org.apache.spark.sql.{SQLContext, SparkSession}

class FactoryJson {

  def read(): Unit = {
    val sparkSession = SparkSession.builder().appName("JSON Reader").master("local").getOrCreate()
    val people = sparkSession.read.json("prueba.json")
    people.createOrReplaceTempView("people")

/*
    val peopleList = people.collectAsList()
*/
    import sparkSession.implicits._

    people.select("name", "age").show

    /*peopleList.forEach {
      println
    }*/

    //people.filter("name" => "Miguel")
    people.groupBy("age").count.show
    //peopleList.stream().filter($"name" == "Claudia").map("$name")


    System.in.read
  }

  def write(): Unit = {

  }

}
