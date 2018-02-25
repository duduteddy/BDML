package matrix

import org.apache.spark.{SparkContext, SparkConf}


object Matrix {
  def main(args: Array[String]): Unit = {
    //创建一个scala版本的SparkContext
    val conf = new SparkConf().setAppName("Matric").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile("F:\\url.txt")
    val M = input.filter(line => line.contains("M"))
    val N = input.filter(line => line.contains("N"))
    //Map阶段，对于矩阵 M，把列作为 key，对于矩阵 N 把行作为 key
    val wordsM = M.map(line => {
      val words = line.split(" ")
      (words(2), words)
    })
    val wordsN = N.map(line => {
      val words = line.split(" ")
      Tuple2(words(1), words)
    })
    // .Reduce阶段，对于相同 key 的值，M 矩阵和 N 矩阵的值做笛卡尔积，
    // 输出 【（M 的行）+ （N 的列值）+ （MN相乘 value 值）】
    val dwords = wordsM.join(wordsN)
    // dwords.values.foreach(println)
    val map = dwords.values.map(x => {
      (x._1(1) + " " + x._2(2), x._1(3).toDouble * x._2(3).toDouble)
    })
    val reduce = map.reduceByKey((x, y) => {
      x + y
    })
    reduce.foreach(x => {
      println(x._1 + " " + x._2)
    })
  }
}
