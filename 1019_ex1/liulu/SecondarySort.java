package com.qst.wordcount;

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

public class SecondarySort {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
		private Text theKey = new Text();

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String[] line = value.toString().split("\\t");
			if (line == null || line.equals(""))
				return;
			if (line.length < 2)
				return;

			String[] year = line[0].split("-");
			if (year == null || year.equals(""))
				return;
			if (year.length < 3)
				return;
			String keyYear = year[0];
			theKey.set(keyYear);
			output.collect(theKey, value);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		private Text v = new Text();
		private Text k = new Text();

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> keylist = new ArrayList<String>();
			List<String> valuelist = new ArrayList<String>();
			while (values.hasNext()) {
				String value = values.next().toString();
				String[] temparture = value.split("\\t");
				if (temparture == null)
					return;
				keylist.add(temparture[0]);
				valuelist.add(temparture[1]);// ÎÂ¶È
			}
			//Collections.sort(valuelist);
			Collections.sort(valuelist, new Comparator<String>() {
				public int compare(String o1, String o2) {
					if (Integer.parseInt(o1) > Integer.parseInt(o2)) {
						return 1;
					}
					if (Integer.parseInt(o1) == Integer.parseInt(o2)) {
						return 0;
					}
					return -1;
				}
			});
			 
			for (int i = 0; i < keylist.size(); i++) {
				k.set(keylist.get(i));
				v.set(valuelist.get(i));
				output.collect(k, v);
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(SecondarySort.class);
		conf.setJobName("WordCount");
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
