package com.qst.wordcount;
/*
 * 将年份和温度生成组合键，通过重写key的默认排序方法进行排序
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
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
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondaryThree {
	public static class MyPair implements WritableComparable<MyPair> {
		private int first;
		private int second;

		public MyPair(int first, int second) {
			super();
			this.first = first;
			this.second = second;
		}

		public void set(int left, int right) {
			first = left;
			second = right;
		}

		public int getFirst() {
			return first;
		}

		public int getSecond() {
			return second;
		}

		public MyPair() {
			super();
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			first = in.readInt();
			second = in.readInt();
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			out.writeInt(first);
			out.writeInt(second);
		}

		@Override
		public int compareTo(MyPair o) {
			// TODO Auto-generated method stub
			if (first != o.first) {
				return first < o.first ? -1 : 1;
			} else if (second != o.second) {
				return second < o.second ? -1 : 1;
			} else {
				return 0;
			}
		}

		public int hashCode() {
			return first * 157 + second;
		}

		public boolean equals(Object right) {
			if (right == null)
				return false;
			if (this == right)
				return true;
			if (right instanceof MyPair) {
				MyPair r = (MyPair) right;
				return r.first == first && r.second == second;
			} else {
				return false;
			}
		}
	}

	public static class FirstPartitioner extends Partitioner<MyPair, Text> {
		@Override
		public int getPartition(MyPair key, Text value, int numPartitions) {
			return Math.abs(key.getFirst() * 127) % numPartitions;
		}
	}
	
	/*
	 * 分组函数，只要first相同就属于同一个组
	 */
	public static class GroupingComparator extends WritableComparator {
		protected GroupingComparator() {
			super(MyPair.class, true);
		}

		@Override
		public int compare(WritableComparable w1, WritableComparable w2) {
			MyPair ip1 = (MyPair) w1;
			MyPair ip2 = (MyPair) w2;
			int l = ip1.getFirst();
			int r = ip2.getFirst();
			return l == r ? 0 : (l < r ? -1 : 1);
		}
	}

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, MyPair, Text> {
		private final MyPair myPair = new MyPair();

		@Override
		public void map(LongWritable key, Text value, OutputCollector<MyPair, Text> output, Reporter reporter)
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
			int temperature = Integer.parseInt(line[1]);
			myPair.set(Integer.parseInt(year[0]), temperature);
			output.collect(myPair, value);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<MyPair, Text, MyPair, Text> {
		private Text k = new Text();
		private Text v = new Text();

		public void reduce(MyPair key, Iterator<Text> values, OutputCollector<MyPair, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				v.set(values.next().toString());
				output.collect(null, v);
			}

		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(SecondaryThree.class);
		conf.setJobName("WordCount");
		conf.setOutputKeyClass(MyPair.class);
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
