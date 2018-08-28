import org.apache.hadoop.hdfs.DFSClient.Conf
import org.apache.spark.{SparkConf, SparkContext}

import scala.annotation.switch

class contextInit(C: String) {
  //(A: String, B: String, C: String) {

  //var appName: String = A
  //var appMaster: String = B
  private var contextType: String = C.toLowerCase()
  //if (contextType == "spark") {
    def sparkContextInit() {
      val conf = new SparkConf().setAppName("sparkApp").setMaster("local[*]")
      var sc: SparkContext = new SparkContext(conf)
      return sc
   // }
  }
}
