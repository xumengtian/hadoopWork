package wc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

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

public class SecSort {
	public static void main(String[] args) throws Exception{
		JobConf conf=new JobConf(WordCount1.class);
        conf.setJobName("SecSort");
        conf.setMapOutputKeyClass(KeyText.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        JobClient.runJob(conf);
	}
	public static class KeyText implements WritableComparable<KeyText>{
			private int key;
			
		 KeyText() {
				super();
			}

			KeyText(int key) {
				this.key = key;
			}

			@Override
			public void readFields(DataInput in) throws IOException {
				// TODO Auto-generated method stub
				key =in.readInt();
			}

			@Override
			public void write(DataOutput out) throws IOException {
				// TODO Auto-generated method stub
				out.writeInt(key);
			}

			@Override
			public int compareTo(KeyText o) {
				// TODO Auto-generated method stub
				return o.key-key;
			}
	}
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, KeyText, Text>{
		//
		@Override
		public void map(LongWritable key, Text value, OutputCollector<KeyText, Text> output, Reporter reporter) throws IOException {
			// TODO Auto-generated method stub
			String line = value.toString();
			StringTokenizer token =new StringTokenizer(line);
			String st[] =token.nextToken().split("-");
			KeyText tk =new KeyText(Integer.parseInt(st[0]));
			output.collect(tk, value);
		}
	}
	public static class Reduce extends MapReduceBase implements Reducer<KeyText,Text,
	Text, Text>{
		@Override
		public void reduce(KeyText key, Iterator<Text> value, OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			// TODO Auto-generated method stub
			List<Integer> list = new ArrayList<Integer>();
			List<String> klist =new ArrayList<String>();
			List<Date_Tem> dtlist =new ArrayList<Date_Tem>();
			while(value.hasNext()){
				String values[] = value.next().toString().split("\t");
				Date_Tem dt = new Date_Tem(Integer.parseInt(values[1]),values[0]);
				dtlist.add(dt);
			}
			Collections.sort(dtlist,new Comparator<Date_Tem>() {

				public int compare(Date_Tem o1, Date_Tem o2) {
					
					
					return o2.getKey()-o1.getKey();
				
					 	
				}
			});
			for (Date_Tem date_Tem : dtlist) {
				output.collect(new Text(date_Tem.getValue()),new Text(""+date_Tem.getKey()));
			}
			
			/*while(value.hasNext()){
				String values[] = value.next().toString().split("\t");
				klist.add(values[0]);
				list.add(Integer.parseInt(values[1]));
			}*/
			//冒泡排序
		/*	for(int i=0;i<list.size()-1;i++){
				for(int j=0;j<list.size()-i-1;j++){
					if(list.get(j)<list.get(j+1)){
						int t = list.get(j);
						list.set(j, list.get(j+1));
						list.set(j+1, t);
						String t1 = klist.get(j);
						klist.set(j, klist.get(j+1));
						klist.set(j+1, t1);
					}
				}
			}*/
				//选择排序
			/*for(int i=0;i<list.size()-1;i++){
				int min =0;
				for(int j=1;j<list.size()-i;j++){
					if(list.get(min)>list.get(j)){
						min=j;
					}
				}
				
					int t = list.get(min);
					list.set(min, list.get(list.size()-1-i));
					list.set(list.size()-1-i, t);
					String t1 = klist.get(min);
					klist.set(min, klist.get(klist.size()-1-i));
					klist.set(klist.size()-1-i, t1);
			}
			for(int i=0;i<list.size();i++){
				
				output.collect(new Text(klist.get(i)), new Text(list.get(i).toString()));
			}*/	
		}
	}
	
}
 class Date_Tem {
	private int key;
	private String value;
	public Date_Tem(int key, String value) {
		super();
		this.key = key;
		this.value = value;
	}
	public int getKey() {
		return key;
	}
	public void setKey(int key) {
		this.key = key;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
	}
	
	
}
