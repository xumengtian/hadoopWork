package tempar_MR;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;


public class MR {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	String line = value.toString();
        	String[] a = line.toString().split("\t");
        	String year = a[0].split("-")[0];
        	word.set(year);
        	output.collect(word,value);
        }
    }
    

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
    	private Text word = new Text();
    	private Text key_tmp = new Text();
    	@Override
    	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
    		HashMap<String,Double> hm = new HashMap<String, Double>();
    		while(values.hasNext()){
        		String v = values.next().toString();
        		hm.put(v.split("-",2)[1],Double.parseDouble(v.split("\t")[1]));
        	}
    		List<HashMap.Entry<String,Double>> list = new ArrayList<HashMap.Entry<String,Double>>(hm.entrySet());
            Collections.sort(list,new Comparator<HashMap.Entry<String,Double>>() {
                public int compare(Entry<String, Double> o1,
                        Entry<String, Double> o2) {
                	return (o1.getValue()).compareTo(o2.getValue()); 
                }
                
            });
    		
    		Iterator it = hm.keySet().iterator();
    		while(it.hasNext()){
    			String tmp = (String) it.next();
    			String[] ans = tmp.split(" ");
    			key_tmp.set(key.toString()+"-"+ans[0]);
    			word.set(ans[1].split("\t")[1]);
    			output.collect(key_tmp,word);
    		}
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(MR.class);
        conf.setJobName("wordcount");
        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }
}

