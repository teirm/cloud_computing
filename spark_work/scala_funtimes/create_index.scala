/* Project: tiny-google
 * Module: create_index 
 * Course: CS1699
 * Date: 11 December 2016
 * Authors: Therese Dachille Cyrus Ramavarapu 
 * Purpose: Search the inverted index created
 *          using Spark given user input
 */

/* FUNCTIONS */
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
/*************************************************/

/* READ IN BOOK DIR AS (BOOK, CONTENT) PairRDD */
val files = sc.wholeTextFiles("../../map_reduce_work/input_dir")

/* REMOVE PUNCTUATION FROM CONTENTS AND UNICODE CHARS */
val cleaned_files = files.mapValues(s => s.replaceAll("""([\p{Punct}&&[^.]]|\b\p{IsLetter}{1,2}\b)\s*""", " ")).mapValues(word => word.replaceAll("\\p{C}", " ")).mapValues(word => word.replaceAll("\\p{P}", " "))

/* SPLIT FILE CONENTS INTO LOWER CASE WORDS */
val split_files = cleaned_files.flatMapValues(line => line.split(" ")).mapValues(word => word.trim.toLowerCase)

/* CREATE STRINGS SO (BOOK:WORD) */
val concat_files = split_files.map(tuple => tuple._1.split("/").last.trim ++ ":" ++ tuple._2.trim)

/* GET WORD COUNTS PER BOOK */
val counts = concat_files.map(key => (key, 1)).reduceByKey(_ + _)

/* SWAP KEYS TO (WORD, BOOK:FREQ) */
val swapped_keys = counts.map(tuple => mix_values(tuple._1, tuple._2))

/* CREATE THE DATA CHAINS */
val inverted_index = swapped_keys.reduceByKey(_ ++ "->" ++ _)

inverted_index.coalesce(1, true).saveAsTextFile("inverted_index")

System.exit(0)
