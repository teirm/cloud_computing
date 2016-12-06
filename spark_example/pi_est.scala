
import org.apache.spark.SparkConf

val sconf = new SparkConf()
val paramString = sconf.get("spark.driver.extraJavaOptions")
val paramSlice = paramString.slice(2,paramString.length)
val part_cnt = paramSlice.toInt  

val count = sc.parallelize(1 to part_cnt).map{i =>
  val x = Math.random()
  val y = Math.random()
  if (x*x + y*y < 1) 1 else 0
}.reduce(_ + _)
println("Pi is roughly " + 4.0 * count / part_cnt)
System.exit(0)
