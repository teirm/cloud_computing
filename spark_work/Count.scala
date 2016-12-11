import java.io.File

    // get list of files in directory
    def getListOfFiles(dir: File, extensions: List[String]):List[File] = {
	   dir.listFiles.filter(_.isFile).toList.filter { file => extensions.exists(file.getName.endsWith(_)) }
    }

    val files = getListOfFiles(new File("../Books/"), List(".txt"))

    for (fi <- files) {
    	var f = sc.textFile(fi.getName)
    	var counts = f.flatMap(line => line.split(" ")).map(word => (f + " " + word,1)).reduceByKey(_+_)
    }

    counts.saveAsTextFile("spark-output")
    System.exit(0)


	    
