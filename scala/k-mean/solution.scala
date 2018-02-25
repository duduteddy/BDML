import scala.collection.mutable.ListBuffer

val size_of_signatures = 100
val shingle_size = 5
val K = 2

val file = sc.textFile("file:/home/of394/bigDataML/b3/Articles.csv")
val content = file.map(x => x.split(",").map(_.trim.toLowerCase)).map(_.mkString(""" """)).map(x => x.sliding(shingle_size).toSet)

val randomNum1Buffer = ListBuffer[Int]()
val randomNum2Buffer = ListBuffer[Int]()

for (i <- 0 until size_of_signatures) {
randomNum1Buffer += scala.util.Random.nextInt(Integer.MAX_VALUE)
randomNum2Buffer += scala.util.Random.nextInt(Integer.MAX_VALUE)
}

val randomNum1 = randomNum1Buffer.toList
val randomNum2 = randomNum2Buffer.toList

val signatures_list_buff = new ListBuffer[List[Int]]()

for (set <- content.collect.toList) {
val signature_list_buff = ListBuffer[Int]()
for (i <- 0 until size_of_signatures) {
var min_hash = Integer.MAX_VALUE
for (shingle <- set) {
var hashcode = Math.abs(((randomNum1(Math.abs(shingle.hashCode() % size_of_signatures)) * Math.abs(shingle.hashCode()) + randomNum2(Math.abs(shingle.hashCode() % size_of_signatures))) % Integer.MAX_VALUE)).toInt
min_hash = Math.min(min_hash, hashcode)
}
signature_list_buff += min_hash
}
signatures_list_buff += signature_list_buff.toList
}

val signatures_list = signatures_list_buff.zipWithIndex.toList

val cluster = ListBuffer[(Int, List[Int])]()

for (i <- 0 until K) {
val rd = scala.util.Random.nextInt(signatures_list.size)
cluster += ((signatures_list(rd)._2, signatures_list(rd)._1))
}

var centroids = cluster.flatMap(x => x.map((k, v) => v))

for (i <- 0 until 10) {
for (seg <- signatures_list) {
val res_cluster = centroids.map(centroid =>  Math.sqrt(centroid.zip(seg._2).map((x, y) => (x - y) * (x - y))).sum)).zipWithIndex.minBy(_._1)._2
cluster(res_cluster) += seg
}
val update = new ListBuffer[List[Int]]()
for (j <- 0 until K) {
val elements = new ListBuffer[Int]()
for(m <- 0 until size_of_signatures){
var sum = 0
for(ele <- cluster(m)){
sum += ele._2(j)
}
elements += sum.toDouble / cluster(j).size
}
update += elements.toList
}
centroids = update.toArray
}

for(n <- 1 until K - 1){
val result = sc.parallelize(cluster(n).map(x => x._1 + """\t""" + x._2))
result.saveAsTextFile("file:/home/of394/bigDataML/b3/output" + c)
}



















