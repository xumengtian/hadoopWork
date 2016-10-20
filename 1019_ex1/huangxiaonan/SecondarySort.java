package hadoop;


import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String[] s =line.split("\t");
			String year = s[0].substring(0,4);
			output.collect(new Text(year),value);
		}
	}
	public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output,Reporter reporter) throws IOException {
			List<Date_Temp> dtlist =new ArrayList<Date_Temp>();
			while(values.hasNext()){
				String[] s = values.next().toString().split("\t");
				Date_Temp dt =new Date_Temp();
				dt.setDate(s[0]);
				dt.setTemp(Integer.parseInt(s[1]));
				dtlist.add(dt);
			}
			Collections.sort(dtlist,new Comparator<Date_Temp>(){

				@Override
				public int compare(Date_Temp o1, Date_Temp o2) {
					// TODO Auto-generated method stub
					return o2.getTemp()-o1.getTemp();
				}
			});
			for (Date_Temp date_Temp : dtlist) {
				output.collect(new Text(date_Temp.getDate()),new Text(""+date_Temp.getTemp()));
			}
		}	
	}
	public static void main(String[] args) throws Exception {
		JobConf conf =new JobConf(SecondarySort.class);
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
class Date_Temp{
	private String date;
	private int  temp;
	public Date_Temp(String date, int temp) {
		super();
		this.date = date;
		this.temp = temp;
	}
	public Date_Temp() {
		super();
	}
	public String getDate() {
		return date;
	}
	public void setDate(String date) {
		this.date = date;
	}
	public int getTemp() {
		return temp;
	}
	public void setTemp(int temp) {
		this.temp = temp;
	}
	
	}

