package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val sp = SparkSession.builder.appName("SimpleApp").getOrCreate()

    // RDD Version
    val csv = sp.sparkContext.textFile("input.csv")
    val map = csv.map{ it =>
      val row = it.split(",")
      Data(Integer.parseInt(row(0)),
           Integer.parseInt(row(1)),
           Integer.parseInt(row(2)))
    }
    val realprice = map.map{it =>
      (it.id, it.price * it.discount / 100)
    }

    val pay = realprice.reduceByKey{(a,b) => a+b}
    val result = pay.collect()
    print("result: ")
    print(result.mkString("Array(", ", ", ")"))

    // Spark SQL Version
    val newNames = Seq("user", "price", "discount")

    val csv2 = sp.read.csv("input.csv").toDF(newNames: _*)
    val rp = csv2.withColumn("realprice", col("price") * col("discount"))
    val sum = rp.groupBy("user").sum("realprice")
    sum.show()
  }
}
