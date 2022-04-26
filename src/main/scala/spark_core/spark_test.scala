package spark_core

import org.apache.spark.{SparkConf, SparkContext}

object spark_test {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("spark_test")
    val sc = new SparkContext(conf)
    val filepath = "D:\\demo.txt"
    val lines = sc.textFile(filepath)
    for(line <- lines){
      println(line)
    }
  }
}
