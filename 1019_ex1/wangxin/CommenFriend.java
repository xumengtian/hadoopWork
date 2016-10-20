package com.qst.cf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

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
import org.apache.hadoop.util.StringUtils;


public class CommenFriend {
	public static class Map extends  MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			
			Text me = new Text();//存储本人
			List<String> friends = new ArrayList<String>();//存储朋友
			
			me.set(tokenizer.nextToken());//将本人传进去
			//--------------------------
			while(tokenizer.hasMoreTokens()){
				friends.add(tokenizer.nextToken());//存储朋友
			}
			
			String sf = StringUtils.join(" ", friends);
			for(int i = 0; i < friends.size();i++){
//				for(int j = i; j < friends.size(); j++){
					int res= (me.toString()).compareTo(friends.get(i));
					if(res >0){
						String s = me +" "+ friends.get(i);
						output.collect(new Text(s), new Text(sf));
					}else if(res  < 0 ){
						String s = friends.get(i)+" "+me;
						output.collect(new Text(s), new Text(sf));
					}
//				}
			}
			
			
			//---------------------
			
			
			
//			while(tokenizer.hasMoreTokens()){
//				friends.add(tokenizer.nextToken());//存储朋友
//			}
//			for(int i = 0; i < friends.size();i++){
//				for(int j = i+1; j < friends.size(); j++){
//					String s = friends.get(i) +" "+ friends.get(j);
//					output.collect(new Text(s), me);
//				}
//			}
		}
	}
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> list = new ArrayList<String>();
			while(values.hasNext()){
				list.add(values.next().toString());
			}
			if(list.size() ==2){
				String[] s1= list.get(0).split(" ");
				String[] s2= list.get(1).split(" ");
			
				List<String> ls = new ArrayList<String>();
				for(int i = 0; i < s1.length;i++){
					for (int j = 0 ;j < s2.length;j++){
						if(s1[i] .equals( s2[j])){
							ls.add(s1[i]);
						}
					}
				}
				output.collect(key, new Text(ls.toString()));
			}
//			if(list == null){
//				output.collect(key, new Text(list.toString()));
//			}else {
//				output.collect(key, new Text(list.toString()));
//			}
		}
	}
	public static void main(String[] args) throws IOException {
		JobConf  conf = new JobConf(CommenFriend.class);
		conf.setJobName("CommenFriend");
		
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
