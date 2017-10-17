package edu.gatech.cse6242

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Q2 {
	case class Graph(source:String, target:String, weight: Int)

def main(args: Array[String]) {
    val sc = new SparkContext(new SparkConf().setAppName("Q2"))
	val sqlContext = new SQLContext(sc)

	import sqlContext.implicits._
	
    val dfpop = sc.textFile("hdfs://localhost:8020" + args(0)).map(_.split("\t")).map(p => Graph(p(0),p(1),p(2).trim.toInt)).toDF()
		
	val data1 = dfpop.filter($"weight" > 1).groupBy(dfpop("source")).agg(sum(dfpop("weight"))).toDF("source","weight1")
	
	val data2 = dfpop.filter($"weight" > 1).groupBy(dfpop("target")).agg(sum(dfpop("weight"))).toDF("target","weight2")

	val data3 = data1.join(data2, data1("source") === data2("target"),"outer")

	val data4 = data3.withColumn("newSource",coalesce($"source",$"target"))

	val data5 = data4.na.fill(0,Seq("weight1","weight2"))

	val data6 = data5.select($"newSource",$"weight2"-$"weight1")
    	// store output on given HDFS path.
    	// YOU NEED TO CHANGE THIS

	val column = data6.map(x => x.mkString("\t"))
    	column.saveAsTextFile("hdfs://localhost:8020" + args(1))
  	}
}
