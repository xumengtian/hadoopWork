package com.qst.friends;

import java.io.IOException;
import java.util.ArrayList;
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

public class CommonFriends2 {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text theKey = new Text();
		private Text theValue = new Text();

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] line = value.toString().split(" ");
			if (line == null || line.equals(""))
				return;
			if (line.length < 1)
				return;
			String person = line[0];
			String friend = "";
			for (int j = 1; j < line.length; j++) {
				if(friend.equals("")){
					friend = line[j];
				}
				else{
					friend = friend + " "+line[j];
				}
			}
			theKey.set(friend);
			for (int i = 1; i < line.length; i++) {
				if(Integer.parseInt(person) < Integer.parseInt(line[i])){
					String comKey = person+","+line[i];
					theValue.set(comKey);
					output.collect(theValue, theKey);
				}else{
					String comKey = line[i]+","+person;
					theValue.set(comKey);
					output.collect(theValue, theKey);
				}
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private Text theValue = new Text();
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> list = new ArrayList<String>();
			String nvalue = "";
			while(values.hasNext()){
				list.add(values.next().toString());
			}
			if(list.size() > 1){
					String[] value1 = list.get(0).split(" ");
					String[] value2 = list.get(1).split(" ");
					for (int i = 0; i < value1.length; i++) {
						for (int j = 0; j < value2.length; j++) {
							if(value1[i].equals(value2[j])){
								if(nvalue.equals("")){
									nvalue = value1[i];
								}
								else{
									nvalue = nvalue +","+value1[i];
								}
							}
						}
					}
			}
			if(!nvalue.equals("")){
				theValue.set("["+nvalue+"]");
				output.collect(key, theValue);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(CommonFriends.class);
		conf.setJobName("Common Friends");
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
