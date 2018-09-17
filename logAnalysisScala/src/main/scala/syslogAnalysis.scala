import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.elasticsearch.spark.sql._
import org.joda.time.format.DateTimeFormat
import org.joda.time.{DateTime, Seconds, format}
import java.util.Date
import java.text.SimpleDateFormat

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object syslogAnalysis {
  var x: String = "2016 Jul 29 03:32:28"
  var y: String = "2017 Jul 29 19:46:12"

  def timeDifference(x: String, y: String): String = {
    //val timeFormat: String = "yyyy MMM dd HH:mm:ss"
    val timeFormat: String = "yyyyMMddHHmmss"
    val timeFormat1: format.DateTimeFormatter = DateTimeFormat.forPattern(timeFormat)
    //var datex = new Date()
    //var datey = new Date()
    //val sdf = new SimpleDateFormat(timeFormat)
    //datex = sdf.parse(x)
    //datey = sdf.parse(y)
    val a: DateTime = DateTime.parse(x, timeFormat1)
    val b: DateTime = DateTime.parse(y, timeFormat1)
    val secs: Int = Seconds.secondsBetween(a, b).getSeconds()
    return math.abs(secs).toString
  }

  var months = List("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
  var monInNumber = 7
  var dayOfMonth = 29
  var weekInNumber = 30

  def main(args: Array[String]): Unit = {
    val sc: SparkContext = if (args(0).contains("runLocal")) {
      val sparkConf = new SparkConf()
      sparkConf.set("spark.broadcast.compress", "false")
      sparkConf.set("spark.shuffle.compress", "false")
      sparkConf.set("spark.shuffle.split.compress", "false")
      sparkConf.set("es.index.auto.create", "true")
      sparkConf.set("es.nodes", "localhost")
      sparkConf.set("es.port", "9200")
      sparkConf.set("es.resource", "syslogAnalysisScala")
      sparkConf.set("es.mapping.id", "userKey")
      sparkConf.set("es.query", "match_all")
      sparkConf.set("es.nodes.wan.only", "false")
      new SparkContext("local[2]", "syslogAnalysis", sparkConf)
    }
    else {
      val sparkConf = new SparkConf()
      sparkConf.set("es.index.auto.create", "true")
      sparkConf.set("es.nodes", "localhost")
      sparkConf.set("es.port", "9200")
      sparkConf.set("es.resource", "syslogAnalysisScala")
      sparkConf.set("es.mapping.id", "userKey")
      sparkConf.set("es.query", "match_all")
      sparkConf.set("es.nodes.wan.only", "false")
      sparkConf.setAppName("syslogAnalysis")
      new SparkContext(sparkConf)
    }
    sc.setLogLevel("ERROR")
    val sqlc = new SQLContext(sc)
    import sqlc.implicits._
    val secureLog = sc.textFile("c:\\data\\secureMixed2.log", 2).persist() //1166245
    val sshdFilter = secureLog.filter(sf => sf.split(" ")(4).length() > 4).
      filter(sf => sf.split(" ")(4).substring(0, 4) == "sshd") //1018878
    val openedClosedFilter = sshdFilter.filter(ocf => List("opened", "closed").contains(ocf.split(" ")(7))) //9735
    val openedClosedFilterDistinct = openedClosedFilter.distinct() //9604
    val openedClosedMap = openedClosedFilterDistinct.map(csm => {
      (csm.split(" ")(0) + " " + csm.split(" ")(1) + " " + csm.split(" ")(3) + " " + csm.split(" ")(4) + " " + csm.split(" ")(10)
        , csm)
    }) // 9604
    val openedSession = openedClosedMap.filter(os => "opened".contains((os._2).split(" ")(7))) //4830
    val closedSession = openedClosedMap.filter(cs => "closed".contains((cs._2).split(" ")(7))) //4774
    val closedSessionKeys = closedSession.keys
    val openedSessionKeys = openedSession.keys
    val closedSessionKeysList = closedSessionKeys.collect().toList
    val openedSessionKeysList = openedSessionKeys.collect().toList
    val openedAndClosedSession = openedSession.filter(oacs => closedSessionKeysList.contains(oacs._1)) //4589
    val openedAndClosedSession2 = closedSession.filter(oacs => openedSessionKeysList.contains(oacs._1)) //4589
    val openedAndClosedSessionUnion = openedAndClosedSession.union(openedAndClosedSession2) //9178
    //    val openedClosedSessionKeyValue = openedAndClosedSessionUnion.
    //      map(ocskv => (ocskv._1, "2017" + " " + ocskv._2.split(" ")(0) + " " + ocskv._2.split(" ")(1) + " " + ocskv._2.split(" ")(2)))
    val openedClosedSessionKeyValue = openedAndClosedSessionUnion.
      map(ocskv => {
        val year: String = "2017"
        val mnth: String = (months.indexOf(ocskv._2.split(" ")(0)) + 1).toString
        var month: String = if (mnth.length < 2) {
          ("0".concat(mnth))
        } else {
          mnth
        }
        val day: String = if (ocskv._2.split(" ")(1).length == 1) {
          "0" + ocskv._2.split(" ")(1)
        }
        else {
          ocskv._2.split(" ")(1)
        }
        val hour: String = ocskv._2.split(" ")(2).substring(0, 2)
        val min: String = ocskv._2.split(" ")(2).substring(3, 5)
        val sec: String = ocskv._2.split(" ")(2).substring(6, 8)
        //val cat: String = (year + month + day + hour + min + sec)
        (ocskv._1, year + month + day + hour + min + sec)
      }) //9176 dist: 8346
    val sessionUsageInSecondsByKey = openedClosedSessionKeyValue.reduceByKey((su1, su2) => timeDifference(su1, su2)) //4588
    //sessionUsageInSecondsBykey.foreach(s => println(s))
    sessionUsageInSecondsByKey.persist()

//    def dfSchema(columnNames: List[String]): StructType =
//      StructType(
//        Seq(
//          StructField(name = "userKey", dataType = StringType, nullable = false),
//          StructField(name = "usageInSeconds", dataType = IntegerType, nullable = false)
//        )
//      )
//
//    def row(line: List[String]): Row = Row(line(0), line(1).toInt)
//    val schema = dfSchema(Seq("userKey", "usageInSeconds"))

    //case class totalUsageSchema(userKey: String, usageInSeconds: Int)
    val sessionUsageInSecondsByKeyDF = sessionUsageInSecondsByKey.toDF("userKey", "usageInSeconds")
      //.map(a => totalUsageSchema(userKey=a._1, usageInSeconds=a._2.toInt)).toDF()
    sessionUsageInSecondsByKeyDF.registerTempTable("totalSessionUsageTable")
    //sessionUsageInSecondsByKeyDF.select("*").where("usageInSeconds != 0").saveToEs("syslogAnalysisScala/log")
    sqlc.sql("select * from totalSessionUsageTable where usageInSeconds != 0").saveToEs("syslogAnalysisScala")
  }
}
