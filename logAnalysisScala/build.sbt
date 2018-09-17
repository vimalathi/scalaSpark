name := "logAnalysisScala"

version := "0.1"

scalaVersion := "2.10.5"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.3"
libraryDependencies += "org.apache.spark" %% "sql" % "1.6.3"

// https://mvnrepository.com/artifact/joda-time/joda-time
libraryDependencies += "joda-time" % "joda-time" % "2.8.1"

//there are vertion mismatch due to no dependencies available for spark 1.6.x
libraryDependencies += "org.elasticsearch" % "elasticsearch-spark_2.10" % "2.3.2"

// https://mvnrepository.com/artifact/org.elasticsearch/elasticsearch
//libraryDependencies += "org.elasticsearch" % "elasticsearch" % "5.3.2"