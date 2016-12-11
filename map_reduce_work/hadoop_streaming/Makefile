build:
	/bin/hadoop com.sun.tools.javac.Main WordCount.java
	jar cf wc.jar WordCount*.class

run1:
	hadoop jar /lib/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -mapper "python ./count_mapper.py" -reducer "python ./count_reducer.py" -input "../input_dir/" -output "./output"  

run2:
	hadoop jar /lib/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -mapper "python ./inv_mapper.py" -reducer "python ./inv_reducer.py" -input "./output" -output "./output_2"	

search:
	hadoop jar /lib/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -mapper "python ./search_mapper.py the your" -reducer "python ./search_reducer.py" -input "./output_2" -output "./search_results"	

top_n:
	hadoop jar /lib/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.7.3.jar -D mapreduce.job.reduces=1 -mapper "python ./top_n_mapper.py 2" -reducer "python ./top_n_reducer.py 2" -input "./search_results" -output "./top_n_results"	

view:
	cat output/part-00000

clean:
	rm -r output
