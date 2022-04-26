package spark_core

import org.apache.spark.{SparkConf, SparkContext}

object wordcount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("wordcount")
    val sc = new SparkContext(conf)
    //读取文件
    val txt = sc.textFile("data/test.txt")
    //拆分为单词
    val words = txt.flatMap(row => row.split(" "))
    //转换为键值对并求值
    val counts = words.map(word => (word,1)).reduceByKey{case(x, y) => x + y}
    //将统计出来的单词总数存入一个文本文件，引发求值
    counts.saveAsTextFile("data/count")
  }
}
