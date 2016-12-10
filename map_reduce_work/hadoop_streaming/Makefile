build:
	/bin/hadoop com.sun.tools.javac.Main WordCount.java
	jar cf wc.jar WordCount*.class

run1:
	hadoop jar /lib/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -mapper "python ./count_mapper.py" -reducer "python ./count_reducer.py" -input "../input_dir/" -output "./output"  

run2:
	hadoop jar /lib/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -mapper "python ./inv_mapper.py" -reducer "python ./inv_reducer.py" -input "./output" -output "./output_2"	

view:
	cat output/part-00000

clean:
	rm -r output
