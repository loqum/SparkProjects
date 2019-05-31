import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

class PresupuestoGeneralitat {

  def run: Unit = {

    val sc = SparkSession
      .builder
      .appName("Presupuesto de la Generalitat")
      .master("local")
      .getOrCreate

    val subAmbit = StructField("subjecte_ambit", DataTypes.StringType)
    val exercici = StructField("exercici", DataTypes.StringType)
    val ingres_despesa = StructField("ingr_s_despesa", DataTypes.StringType)
    val servei_entitat = StructField("servei_entitat", DataTypes.StringType)
    val nom_servei_entitat = StructField("nom_servei_entitat", DataTypes.StringType)
    val subsector = StructField("subsector", DataTypes.StringType)
    val codi_agrupaci = StructField("codi_agrupaci", DataTypes.StringType)
    val nom_agrupaci = StructField("nom_agrupaci", DataTypes.StringType)
    val codi_secci = StructField("codi_secci", DataTypes.StringType)
    val nom_secci = StructField("nom_secci", DataTypes.StringType)
    val tipus_de_secci = StructField("tipus_de_secci", DataTypes.StringType)
    val entitat_cp_sector_p_blic = StructField("entitat_cp_sector_p_blic", DataTypes.StringType)
    val ordre_departamental = StructField("ordre_departamental", DataTypes.StringType)
    val codi_entitat = StructField("codi_entitat", DataTypes.StringType)
    val nom_entitat = StructField("nom_entitat", DataTypes.StringType)
    val cap_tol = StructField("cap_tol", DataTypes.StringType)
    val nom_cap_tol = StructField("nom_cap_tol", DataTypes.StringType)
    val article = StructField("article", DataTypes.StringType)
    val nom_article = StructField("nom_article", DataTypes.StringType)
    val concepte = StructField("concepte", DataTypes.StringType)
    val nom_concepte = StructField("nom_concepte", DataTypes.StringType)
    val aplicacio = StructField("aplicacio", DataTypes.StringType)
    val nom_aplicaci = StructField("nom_aplicaci", DataTypes.StringType)
    val codi_rea = StructField("codi_rea", DataTypes.StringType)
    val nom_rea = StructField("nom_rea", DataTypes.StringType)
    val codi_pol_tica = StructField("codi_pol_tica", DataTypes.StringType)
    val nom_pol_tica = StructField("nom_pol_tica", DataTypes.StringType)
    val codi_programa = StructField("codi_programa", DataTypes.StringType)
    val nom_programa = StructField("nom_programa", DataTypes.StringType)
    val import_sense_consolidar = StructField("import_sense_consolidar", DataTypes.DoubleType)
    val import_consolidat_sector_p_blic = StructField("import_consolidat_sector_p_blic", DataTypes.DoubleType)


        val fields = Array(subAmbit, exercici, ingres_despesa, servei_entitat, nom_servei_entitat, subsector, codi_agrupaci,
          nom_agrupaci, codi_secci, nom_secci, tipus_de_secci, entitat_cp_sector_p_blic, ordre_departamental, codi_entitat, nom_entitat, cap_tol,
          nom_cap_tol, article, nom_article, concepte, nom_concepte, aplicacio, nom_aplicaci, codi_rea, nom_rea, codi_pol_tica,
          nom_pol_tica, codi_programa, nom_programa, import_sense_consolidar, import_consolidat_sector_p_blic)

/*
    val fields = Array(import_sense_consolidar, import_consolidat_sector_p_blic)
*/

    val schema = StructType(fields)

    val dataFrame = sc
      .read
      .schema(schema)
      .option("header", "true")
      .csv("C:\\Users\\ruben\\Desktop\\gastos_gene.csv")

    val features = new VectorAssembler().setInputCols(Array("import_sense_consolidar", "import_consolidat_sector_p_blic")).setOutputCol("features")

    val linearRegression = new LinearRegression().setLabelCol("import_consolidat_sector_p_blic")
    val pipeline = new Pipeline().setStages(Array(features, linearRegression))
    val model = pipeline.fit(dataFrame)

    val result = model.transform(dataFrame).select("import_sense_consolidar", "import_consolidat_sector_p_blic", "prediction")

    val tsvWithHeaderOptions: Map[String, String] = Map(
      ("delimiter", "\t"),
      ("header", "true"))

    result.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions)
      .csv("outputTemperatureCSV")

    result.show
    dataFrame.show
    import sc.sqlContext.implicits._
    //val wordsRDD = dataFrame.rdd.flatMap(_.split(" ")).map(word => (word.toLowerCase, 1)).reduceByKey(_ + _)


  }

}
