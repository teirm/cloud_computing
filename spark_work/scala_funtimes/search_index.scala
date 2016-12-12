/* Project: tiny-google
 * Module: spark-search
 * Course: CS1699
 * Date: 11 December 2016
 * Authors: Therese Dachille Cyrus Ramavarapu 
 * Purpose: Search the inverted index created
 *          using Spark given user input
 */


/* NOTE: THESE NEEDS TO BE SET BY THE USER INPUT */
val search_terms = Array("the","your")
val max_rank = 1 
/*************************************************/

/* FUNCTIONS */
def search_file(data_line: String) = search_terms contains data_line.split(",")(0)

def get_top_n(search_line: String): (String) = {

  val search_term = search_line.split(",")(0)
  val book_results = search_line.split(",")(1)
  val data_nodes = book_results.split("->") 
  val tupled_nodes = data_nodes.map(node => (node.split(":")(0), node.split(":")(1))) 
  val sorted_nodes = tupled_nodes.sortWith(_._2 > _._2)     
  
  val top_results = sorted_nodes.slice(0, max_rank)  
  
  return search_term ++ ":" ++ top_results.mkString 
}
/*************************************************/

/* READS IN THE INVERTED INDEX */
val inv_index = sc.textFile("inverted_index")

/* REMOVES PARENTHESES FROM INVERTED INDEX */
val cleaned_index = inv_index.map(x => x.replaceAll("[()]",""))

/* FILTERS INDEX TO FIND SEARCH TERMS */
val search_results = cleaned_index.filter(search_file)

/* GETS THE TOP N RESULTS BASED UPON THE SEARCH TERMS */
val top_n_results = search_results.map(line => get_top_n(line))

/* SAVES THE RESULTS TO A FILE */
top_n_results.coalesce(1, true).saveAsTextFile("search_results")

System.exit(0)
