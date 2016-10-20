package commonfriend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

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


public class commonfriend {
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
        	
        	String line = value.toString();
        	String[] tmp = line.split(" ");
        	String keyses = line.split(" ",2)[1];
        	
        	Text owner = new Text();
        	List<String> set = new ArrayList<String>();
        	
        	for(String s:tmp){
        		set.add(s);
        	}
        	
        	String[] friends = new String[set.size()];
        	friends = set.toArray(friends);
        	
        	for(int i=1;i<friends.length;i++){
        		String nextkey = "";
        		if(Integer.parseInt(friends[0])<Integer.parseInt(friends[i])){
        			nextkey = friends[0]+","+friends[i];
        		}else{
        			nextkey = friends[i]+","+friends[0];
        		}
        		owner.set(nextkey);
        		word.set(keyses);
        		output.collect(owner, word);
        	}
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

		private Text word = new Text();

		@Override
		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			List<String> commonfriends = new ArrayList<String>();
			boolean flag=true;
			while(values.hasNext()){
				Text a = values.next();
				List<String> tmp_list = new ArrayList<String>();
				String[] val = a.toString().split(" ");
				for(String s:val){
					tmp_list.add(s);
				}
				if(flag){
					commonfriends = tmp_list;
					flag=false;
				}else{
					commonfriends.retainAll(tmp_list);
				}
			}
			String tmp = "";
			for(String s:commonfriends){
				tmp+=s+",";
			}
			if(tmp!=""){
				tmp="["+tmp.substring(0,tmp.length()-1)+"]";
				word.set(tmp);
				output.collect(key,word);
			}
		}
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(commonfriend.class);
        conf.setJobName("commonfirend");
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
