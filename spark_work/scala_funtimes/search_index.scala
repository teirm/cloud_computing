/* Project: tiny-google
 * Module: spark-search
 * Course: CS1699
 * Authors: Therese Dachille Cyrus Ramavarapu 
 * Purpose: Search the inverted index created
 *          using Spark given user input
 */
val search_terms = Array("the","your")

val max_rank = 2 

def search_file(data_line: String) = search_terms contains data_line.split(",")(0)

def get_top_n(search_line: String): (String) = {

  val search_term = search_line.split(",")(0)
  val book_results = search_line.split(",")(1)
  val data_nodes = book_results.split("->") 
  val tupled_nodes = data_nodes.map(node => (node.split(":")(0), node.split(":")(1))) 
  val sorted_nodes = tupled_nodes.sortWith(_._2 > _._2)     
  
  val top_results = sorted_nodes.slice(0, max_rank)  
  
  return search_term ++ top_results.mkString 
}

val inv_index = sc.textFile("inverted_index")
val cleaned_index = inv_index.map(x => x.replaceAll("[()]",""))
val search_results = cleaned_index.filter(search_file)
val top_n_results = search_results.map(line => get_top_n(line))

top_n_results.foreach(println)

System.exit(0)
