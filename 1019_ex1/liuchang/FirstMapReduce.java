package Sort;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.examples.SecondarySort.Reduce;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.MRBench.Map;


public class FirstMapReduce {

	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] splits =line.split("-", 2);
			output.collect(new Text(splits[0]), new Text(splits[1]));
		}
		
		
		
	}
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{


			List<Glue> list =new ArrayList<Glue>();
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			Glue glue= null;
			//List<Glue> list =new ArrayList<Glue>();
			
			//IdentityHashMap<Double, String> map = new IdentityHashMap<Double, String>(); 
			while (values.hasNext()){
				glue= new Glue();
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
				//String s=key.toString();
				//String ss=s+g.getDate();
				//output.collect(new Text(ss), new Text(g.getTemp()));
				output.collect(key, new Text(g.getDate()+" "+g.getTemp()));
			}
			
		}
		
	}
	public static void main(String[] args) throws Exception {
		JobConf conf =new JobConf(FirstMapReduce.class);
		conf.setJobName("FirstMapReduce");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
		
	}
}
