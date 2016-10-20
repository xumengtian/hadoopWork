package demo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

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
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
public class Demo_03 {
	public static class Mapp extends MapReduceBase implements Mapper<LongWritable , Text,
	Text, Text>{
		private Text raw_key = new Text();
		private Text raw_friends = new Text();
		@Override
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
						throws IOException {
			String line = value.toString();
			String [] col = line.split(" ");
			String [] col_2 = line.split(" ",2);
			raw_friends.set(col_2[1]);
			for(int i = 1; i < col.length; i++){
				if(Integer.parseInt(col[0]) <= Integer.parseInt(col[i])){
					raw_key.set(col[0]+","+col[i]);
				}else{
					raw_key.set(col[i]+","+col[0]);
				}
				output.collect(raw_key, raw_friends);
			}
		}
	}
	
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, 
	Text, Text> {
		ArrayList<Map<String,String>> record = new ArrayList<Map<String,String>>();
		Map<String, String> m = null;
		StringBuffer sb = new StringBuffer();
		@Override
		public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
			record.clear();
			sb.delete(0,sb.length());
			while(values.hasNext()){
				m = new HashMap<String, String>();
				String[] raw = new Text(values.next()).toString().split(" ");
				for(String a : raw){
					m.put(a, a);
				}
				record.add(m);
			}
			Iterator<String> k1 = record.get(0).keySet().iterator();
			if(record.size() > 1){
				while(k1.hasNext()){
					String kk = k1.next();
					if(record.get(1).containsKey(kk)){
						sb.append(kk+",");
					}
				}
			}
			if(sb.length()!=0){
				sb.replace(sb.length()-1, sb.length(), "");
				output.collect(key, new Text("["+sb.toString()+"]"));
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Demo_03.class);
		conf.setJobName("WordCount");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		
		conf.setMapperClass(Mapp.class);
		conf.setReducerClass(Reduce.class);
		conf.setNumReduceTasks(1);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
	}
}
