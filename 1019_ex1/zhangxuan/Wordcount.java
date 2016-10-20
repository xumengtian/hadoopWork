package wc;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
public class Wordcount {

	public static class Map extends MapReduceBase implements Mapper<LongWritable , Text,
	Text, Text>{
		//private Text word = new Text();
		//private final static IntWritable one = new IntWritable(1);
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			String line = value.toString();
			//StringTokenizer tokenizer = new StringTokenizer(line);
			String []splits =line.split("-",2);
			
			//while (tokenizer.hasMoreTokens()) {
				//word.set(tokenizer.nextToken());
				output.collect(new Text(splits[0]),new Text(splits[1]));
		//	}
		}
	}

	
	public static class MyPartitioner extends MapReduceBase 
	implements Partitioner<Text, Text> {

		@Override
		public int getPartition(Text key, Text value, int reduceNum) {
			String str = key.toString();
			if (str.startsWith("1970")){
				return 0;
			}else if (str.startsWith("1971")){
				return 1;
			}else if (str.startsWith("1972")){
				return 2;
			}else{
				return 3;
			}
		}
	}
	
	

	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, 
	Text, Text> {	
		Glue glue= new Glue();
		List<Glue> list =new ArrayList<Glue>();
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()){
				String []split=values.next().toString().split("\t", 2);
				glue.setDate(split[0]);
				glue.setTemp(split[1]);
				list.add(glue);
			}
			Collections.sort(list,new Comparator<Glue>() {
				public int compare(Glue o1, Glue o2) {
					return Integer.parseInt(o1.getTemp())-Integer.parseInt(o2.getTemp());
				}
			});
			for (Glue g : list) {
				output.collect(key, new Text(g.getDate()+" "+g.getTemp()));
			}
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Wordcount.class);
		conf.setJobName("WordCount");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setPartitionerClass(MyPartitioner.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(4);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}


}
