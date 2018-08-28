import org.apache.spark.{SparkContext, SparkConf}

object syslogAnalysis {
  def main(args: Array[String]): Unit = {
    val sc: SparkContext = if(runLocal){
      val sparkConf = new SparkConf()
      sparkConf.set("spark.broadcast.compress", "false")
      sparkConf.set("spark.shuffle.compress", "false")
      sparkConf.set("spark.shuffle.split.compress", "false")
      new SparkContext("local[2]", "syslogAnalysis", sparkConf)
    }
    else{
      val sparkConf = new SparkConf().setAppName("syslogAnalysis")
      new SparkContext(sparkConf)
    }
    val secureLog = sc.textFile("c:\\data\\secureMixed.log")
    val sshdFilter = secureLog.filter(sf => (String:(sf.split(" ")[4])[:4]). == "sshd")
  }

}
