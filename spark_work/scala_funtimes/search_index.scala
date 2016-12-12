/* Project: tiny-google
 * Module: spark-search
 * Course: CS1699
 * Authors: Therese Dachille Cyrus Ramavarapu 
 * Purpose: Search the inverted index created
 *          using Spark given user input
 */
val search_term : String = "the"

def search_file(data_line: String) = data_line.split(",")(0) == "the" 


val inv_index = sc.textFile("inverted_index")
val cleaned_index = inv_index.map(x => x.replaceAll("[()]",""))
val search_results = cleaned_index.filter(search_file)

search_results.coalesce(1, true).saveAsTextFile("search_results")

System.exit(0)
