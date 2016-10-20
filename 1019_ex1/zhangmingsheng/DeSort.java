package join;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
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

public class Sort {
	public static class Map extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line = value.toString();
			String str[] = line.split("-", 2);
			output.collect(new Text(str[0]), new Text(line));
		}
	}

	public static class Reduce extends MapReduceBase implements
			Reducer<Text, Text, Text, Text> {
		private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter report)
				throws IOException {
			List<Temperature> list = new ArrayList<Temperature>();
			while (values.hasNext()) {
				String line = values.next().toString();
				String str[] = line.split("\t", 2);
				Temperature temperature = new Temperature(str[0], Integer
						.parseInt(str[1]));
				list.add(temperature);
			}

			try {
				Collections.sort(list, new Comparator<Temperature>() {
					public int compare(Temperature temp1, Temperature temp2) {
						int poor1 = temp2.getTemperature()
								- temp1.getTemperature();
						
							Date data1 = null;
							Date data2 = null;
							try {
								data1 = sdf.parse(temp1.getDate());
								data2 = sdf.parse(temp2.getDate());
							} catch (ParseException e) {
								e.printStackTrace();
							}
						String sdate1=(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(data1);
						String sdate2=(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")).format(data2);
						int poor2 = poor1 == 0 ? sdate2.compareTo(
								sdate1) : poor1;
						return poor2;
					}
				});

				for (Temperature temp : list) {
					String st = temp.getDate() + "\t" + temp.getTemperature();
					output.collect(new Text(st), new Text(""));
				}
			} catch (Exception e) {

			}
		}
	}

	public static void main(String[] args) throws IOException {
		JobConf conf = new JobConf(Sort.class);
		conf.setJobName("Sort");
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
