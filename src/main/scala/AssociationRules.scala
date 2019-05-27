import org.apache.spark.mllib.fpm.AssociationRules
import org.apache.spark.mllib.fpm.FPGrowth.FreqItemset
import org.apache.spark.{SparkConf, SparkContext}

object AssociationRules {

  def main(args: Array[String]) {
    run()
  }

  def run(): Unit = {
    val conf = new SparkConf().setAppName("AssociationRulesExample").setMaster("local[2]").set("spark.executor.memory", "1g")
    val sc = new SparkContext(conf)

    // $example on$
    val freqItemsets = sc.parallelize(Seq(
      new FreqItemset(Array("a"), 15L),
      new FreqItemset(Array("c"), 15L),
      new FreqItemset(Array("a", "c"), 45L),
    ))

    val ar = new AssociationRules()
      .setMinConfidence(0.8)
    val results = ar.run(freqItemsets)

    results.collect().foreach { rule =>
      println(s"[${rule.antecedent.mkString(",")}=>${rule.consequent.mkString(",")} ]" +
        s" ${rule.confidence}")
    }
    // $example off$

    System.in.read
    //sc.stop()
  }

}
