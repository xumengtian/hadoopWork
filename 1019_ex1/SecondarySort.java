package ss;

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


public class SecondarySort {
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable , Text,
			Text, Text>{

		@Override
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter report)
				throws IOException {
			// TODO Auto-generated method stub
			String line = value.toString();
			String[] s = line.split("-",2);
			output.collect(new Text(s[0]),new Text(line));			
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, 
		Text, Text> {
		private static final Text SEPARATOR = new Text("--------------------------------------");
		@Override
		public void reduce(Text key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter report)
				throws IOException {
			output.collect(SEPARATOR, null);
			
			List<String>list = new ArrayList<String>();
			while(value.hasNext()){
				String line = value.next().toString();
				list.add(line);
			}
			for(int i = 0;i<list.size();i++){
				for(int j = 0; j < list.size()-i-1;j++){
					String s1[] = list.get(j).split("\t",2);
					String s2[] = list.get(j+1).split("\t",2);
					if(Integer.parseInt(s1[1])>Integer.parseInt(s2[1])){
						String tmp=list.get(j);
						list.set(j,list.get(j+1));
						list.set(j+1, tmp);
						}
					}
				}	
				for(String s:list){
					String str[]=s.split("\t", 2);
					output.collect(new Text(str[0]),new Text(str[1]));
				}
			}
		}
		
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(SecondarySort.class);
		conf.setJobName("SecondarySort");
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
