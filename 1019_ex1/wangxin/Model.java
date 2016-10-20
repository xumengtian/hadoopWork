package com.qst.MR_Hot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

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

public class Model {

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text k = new Text();
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {

			String line = value.toString();
			String[] st = line.split("-");
			String year = st[0];
			k.set(year);
			output.collect(k, value);
		
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		
//		private Text text = new Text();
		@Override
		public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> list = new ArrayList<String>();
			while(value.hasNext()){
				String str = value.next().toString();
				list.add(str);
			}
			Collections.sort(list, new Comparator<String>() {

				@Override
				public int compare(String o1, String o2) {
					int t1 =  Integer.parseInt(o1.split("\t")[1]);
					int t2 =  Integer.parseInt(o2.split("\t")[1]);
					if (t1 == t2)
						return 0;
					return t1 > t2 ? -1 : 1;
				}
			});
			for (String str : list) {
				String [] s =str.split("\t");
				
				output.collect(new Text(s[0]), new Text(s[1]));
			}
		}
		

	}

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(Model.class);
		conf.setJobName("Medel");
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);

	}
}
