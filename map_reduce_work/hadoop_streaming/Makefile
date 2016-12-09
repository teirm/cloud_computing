build:
	/bin/hadoop com.sun.tools.javac.Main WordCount.java
	jar cf wc.jar WordCount*.class

run:
	hadoop jar /lib/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -mapper "python ./mapper.py" -reducer "python ./reducer.py" -input "../word_count/input_dir/BeowulfbyJLesslieHall.txt" -out "./output"  

view:
	cat output_dir/part-r-00000

clean:
	rm -r output_dir
