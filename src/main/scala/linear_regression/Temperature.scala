package linear_regression

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object Temperature {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("Linear Regression: Temperature")
      .master("local")
      .getOrCreate

    val temperatura = StructField("temperatura", DataTypes.IntegerType)
    val defecto = StructField("defecto", DataTypes.IntegerType)
    val fields = Array(temperatura, defecto)
    val schema = StructType(fields)

    val dataFrame = spark
      .read
      .schema(schema)
      .option("header", "true")
      .csv("temperatura.csv")

    val features = new VectorAssembler().setInputCols(Array("temperatura")).setOutputCol("features")
    val linearRegression = new LinearRegression().setLabelCol("defecto")
    val pipeline = new Pipeline().setStages(Array(features, linearRegression))
    val model = pipeline.fit(dataFrame)

    /*val linearRegressionModel = model.stages(1).asInstanceOf[LinearRegressionModel]

    println(s"RMSE:  ${linearRegressionModel.summary.rootMeanSquaredError}")
    println(s"r2:    ${linearRegressionModel.summary.r2}")
    println(s"Model: Y = ${linearRegressionModel.coefficients(0)} * X + ${linearRegressionModel.intercept}")

    linearRegressionModel.summary.residuals.show*/

    val result = model.transform(dataFrame).select("temperatura", "defecto", "prediction")

    val csvWithHeaderOptions: Map[String, String] = Map(
      ("delimiter", ","),
      ("header", "true"))

    val tsvWithHeaderOptions: Map[String, String] = Map(
      ("delimiter", "\t"),
      ("header", "true"))

    result.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(csvWithHeaderOptions)
      .csv("outputTemperatureCSV")

    result.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions)
      .csv("outputTemperatureTSV")

    result.coalesce(1)
        .write
        .mode(SaveMode.Overwrite)
        .json("outputTemperatureJSON")

    result.show

  }

}
