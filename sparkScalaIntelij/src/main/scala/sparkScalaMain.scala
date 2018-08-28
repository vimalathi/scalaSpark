import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object sparkScalaMain {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = if(runLocal){
      val sparkConf = new SparkConf()
      sparkConf.set("spark.broadcast.compress", "false")
      sparkConf.set("spark.shuffle.compress", "false")
      sparkConf.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[2]", "sparkApp", sparkConf)
    }else{
      val sparkConf = new SparkConf().setAppName("sparkApp")
      new SparkContext(sparkConf)
    }
    val bc = sc.broadcast(scala.io.Source.fromFile("filterFile").getLines.toSet)
    val inputRDD = sc.textFile("c:\\data\\secureMixed.log")
    val filteredWordcount: RDD[(String, Int)] = filterAndCount(bc, inputRDD)
    filteredWordcount.collect().foreach(case(name: String, count: Int) => println("-"+ name + ":" + count))
  }

}
