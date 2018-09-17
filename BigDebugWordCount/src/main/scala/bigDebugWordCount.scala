//https://sites.google.com/site/sparkbigdebug/debugging-wordcount
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.bdd.BigDebugConfiguration
import org.apache.spark.lineage.LineageContext

object bigDebugWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length == 0) {
      println("<runLocal>")
      return
    }

    val sc: SparkContext = if (args(0).contains("runLocal")) {
      val sparkConf = new SparkConf()
      sparkConf.set("spark.broadcast.compress", "false")
      sparkConf.set("spark.shuffle.compress", "false")
      sparkConf.set("spark.shuffle.split.compress", "false")
      sparkConf.setMaster("local[2]")
      sparkConf.setAppName("BigDebugWordCount")
      new SparkContext(sparkConf)
    } else {
      val sparkConf = new SparkConf().setAppName("BigDebugWordCount")
      new SparkContext(sparkConf)
    }
    //big debug configuration
    val bdConf = new BigDebugConfiguration

    val file = sc.textFile("C:\\data\\secure.txt")
    val fm = file.flatMap(line => line.trim().split(" "))
    val pair = fm.map { word => (word, 1) }
    val count = pair.reduceByKey(_ + _)
    count.collect().foreach(println)
  }
}
