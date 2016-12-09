build:
	/bin/hadoop com.sun.tools.javac.Main WordCount.java
	jar cf wc.jar WordCount*.class

run:
	hadoop jar wc.jar WordCount input_dir/ output_dir/

view:
	cat output_dir/part-r-00000

clean:
	rm -r output_dir
