import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


val conf = new SparkConf()
val sc = new SparkContext(conf)

val files = sc.wholeTextFiles("../../map_reduce_work/input_dir")

//val splits = files.mapValues(line => line.split(" ")) 

//files.keys.map(s => s.split("/").last).collect.foreach(println)
//files.values.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).collect.foreach(println)

//val got_books = files.mapKeys(s => s.split("/").last)

val cleaned_files = files.mapValues(s => s.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ")).mapValues(word => word.replaceAll("\\p{C}", " ")).mapValues(word => word.replaceAll("\\p{P}", " "))
val split_files = cleaned_files.flatMapValues(line => line.split(" ")).mapValues(word => word.trim)
val stripped_files = split_files.mapValues(word => word.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", "")).mapValues(word => word.replaceAll("\\p{C}", ""))
val concat_files = stripped_files.map(tuple => tuple._1.split("/").last.trim ++ ":" ++ tuple._2.trim)

val counts = concat_files.map(key => (key, 1)).reduceByKey(_ + _)


counts.coalesce(1, true).saveAsTextFile("test_file.txt")

System.exit(0)
