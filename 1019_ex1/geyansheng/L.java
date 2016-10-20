package hot;

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


public class L {
public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{

		
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] s =line.split("-");
			String year = s[0];
			output.collect(new Text(year),new Text(line));
		}
	
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			
			List<Date_Tmp> dtlist = new ArrayList<Date_Tmp>();
			while(values.hasNext()){
				String [] s = values.next().toString().split("\t");
				Date_Tmp dt = new Date_Tmp();
				dt.setDate(s[0]);
				dt.setTmp(Integer.parseInt(s[1]));
				dtlist.add(dt);
				}
			
			Collections.sort(dtlist,new Comparator<Date_Tmp>(){

				public int compare(Date_Tmp o1, Date_Tmp o2) {
					return o2.getTmp()-o1.getTmp();
				}	
			});
			
			for (Date_Tmp a : dtlist) {
				output.collect(new Text(a.getDate()),new Text(""+a.getTmp()));
			}
			}
		
	}
	 
	public static void main(String[] args) throws Exception {
		JobConf conf =new JobConf(L.class);
		conf.setJobName("L");
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
class Date_Tmp{
	private String date;
	private int tmp;
	public Date_Tmp(String date, int tmp) {
		super();
		this.date = date;
		this.tmp = tmp;
	}
	public Date_Tmp() {
		super();
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public int getTmp() {
		return tmp;
	}
	public void setTmp(int tmp) {
		this.tmp = tmp;
	}
	
}