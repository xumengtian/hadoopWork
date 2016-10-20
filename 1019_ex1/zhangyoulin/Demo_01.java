package demo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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

public class Demo_01 {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
		private Text key_year = new Text();
		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] colValue = line.split("\\t");
			key_year.set(colValue[0].substring(0, 4));
			output.collect(key_year, value);
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, 
	Text, Text> {
		ArrayList<Text> record = new ArrayList<Text>();
		Text date = new Text();
		Text temp = new Text();
		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			record.clear();
			while (values.hasNext()){
				record.add(new Text(values.next()));
			}
			Collections.sort(record, new Comparator<Text>() { 
				public int compare(Text a, Text b) { 
					String[] rawA = a.toString().split("\\t");
					String[] rawB = b.toString().split("\\t");
					int orderA = Integer.parseInt(rawA[1]); 
					int orderB = Integer.parseInt(rawB[1]); 
					return orderA - orderB; 
				} 
			});
			for(Text rec : record){
				String[] raw = rec.toString().split("\\t");
				date.set(raw[0]);
				temp.set(raw[1]);
				output.collect(date, temp);
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Demo_01.class);
		conf.setJobName("Demo_01");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(1);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
