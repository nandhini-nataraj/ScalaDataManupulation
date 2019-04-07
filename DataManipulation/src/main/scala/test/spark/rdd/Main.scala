package test.spark.rdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Main {
  def main(args:Array[String]){
    val conf = new SparkConf()
    conf.set("spark.master","local")
    conf.set("spark.app.name", "SparkJDDApp")
    val sc = new SparkContext(conf)
    val rl = 10.to(13)
    println(rl)
    sc.stop()
  }
}