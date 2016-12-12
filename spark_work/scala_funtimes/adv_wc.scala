import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

def mix_values(concat_val: String, frequency: Int):
(String, String) = {

  var word : String = ""
  var book_freq : String = ""

  if (concat_val.split(":").length == 2) {
  
    val book_name = concat_val.split(":")(0)
    word = concat_val.split(":")(1) 
    book_freq = book_name ++ ":" ++ frequency.toString

  
  } else {
    val book_name = concat_val.split(":")(0)
    word = "~SPACE~" 
    book_freq = book_name ++ ":" ++ frequency.toString
   } 
  
  return (word, book_freq)
}


val conf = new SparkConf()
val sc = new SparkContext(conf)

val files = sc.wholeTextFiles("../../map_reduce_work/input_dir")

val cleaned_files = files.mapValues(s => s.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ")).mapValues(word => word.replaceAll("\\p{C}", " ")).mapValues(word => word.replaceAll("\\p{P}", " "))
val split_files = cleaned_files.flatMapValues(line => line.split(" ")).mapValues(word => word.trim.toLowerCase)
val stripped_files = split_files.mapValues(word => word.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", "")).mapValues(word => word.replaceAll("\\p{C}", ""))
val concat_files = split_files.map(tuple => tuple._1.split("/").last.trim ++ ":" ++ tuple._2.trim)
val counts = concat_files.map(key => (key, 1)).reduceByKey(_ + _)
val swapped_keys = counts.map(tuple => mix_values(tuple._1, tuple._2))
val inverted_index = swapped_keys.reduceByKey(_ ++ "->" ++ _)

inverted_index.coalesce(1, true).saveAsTextFile("inverted_index")

System.exit(0)
