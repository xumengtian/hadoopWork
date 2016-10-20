package com.qst.wordcount;
/*
 * 通过冒泡排序对Value进行排序
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
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

public class secondarytwo {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, MyText, Text> {
		@Override
		public void map(LongWritable key, Text value, OutputCollector<MyText, Text> output, Reporter reporter)
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
			//theKey.set(keyYear);
			MyText theKey = new MyText(keyYear);
			output.collect(theKey, value);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<MyText, Text, Text, Text> {
		private Text v = new Text();
		private Text k = new Text();
		public void reduce(MyText key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> keylist = new ArrayList<String>();
			List<String> valuelist = new ArrayList<String>();
			while (values.hasNext()) {
				String value = values.next().toString();
				String[] temparture = value.split("\\t");
				if (temparture == null)
					return;
				keylist.add(temparture[0]);
				valuelist.add(temparture[1]);// 温度
			}
			for (int i = 0; i < valuelist.size() - 1; i++) {
				for (int j = 0; j < valuelist.size() - i - 1; j++) {
					if (Integer.parseInt(valuelist.get(j)) < Integer.parseInt(valuelist.get(j + 1))) {
						String str = "a";
						str = valuelist.get(j);
						valuelist.set(j, valuelist.get(j + 1));
						valuelist.set(j + 1, str);

						str = keylist.get(j);
						keylist.set(j, keylist.get(j + 1));
						keylist.set(j + 1, str);
					}
				}
			}
			for (int i = 0; i < keylist.size(); i++) {
				v.set(valuelist.get(i));
				k.set(keylist.get(i));
				output.collect(k, v);
			}
		}
	}

	public static class MyText implements WritableComparable<MyText> {
		private String key;
		
		public MyText(String key) {
			super();
			this.key = key;
		}
		public MyText() {
		}

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		@Override
		public void readFields(DataInput arg0) throws IOException {
			// TODO Auto-generated method stub
			key = arg0.readUTF();
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			// TODO Auto-generated method stub
			arg0.writeUTF(key);
		}
		@Override
		public int compareTo(MyText o) {
			// TODO Auto-generated method stub
			return Integer.parseInt(o.key)-Integer.parseInt(key);
		}

	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(secondarytwo.class);
		conf.setJobName("WordCount");
		conf.setOutputKeyClass(MyText.class);
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
