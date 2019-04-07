package test.spark.rdd

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object LoadCSVFile {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder()
      .appName("Creating RDD with CSV file")
      .master("local")
      .getOrCreate()

    //load train df
    val df = sparkSession.read.option("header", "true").option("inferSchema", "true").csv("src/main/resources/dataset/train.csv")
    df.printSchema()

    // Counts people by age
    val countsByAge = df.groupBy("age").count()
    countsByAge.show()

    countsByAge.coalesce(1).write.format("csv").option("header", "true").mode(SaveMode.Overwrite).save(args(0))
  }
}