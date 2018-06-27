package PMI_Mapper;
//second 计算属性熵
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class PMI_Comput {

	public static class MyMapper extends Mapper<LongWritable,Text,Text, Text>{
		private Text word =new Text();
		private Text word1 =new Text();
	        public void map(LongWritable key ,Text value,Context context)throws IOException, InterruptedException{
			String line = value.toString();
			String[] str =line.split(",")	;
			word.set(str[0]);
		   word1.set(str[2]+","+str[3]);
		   context.write(word,word1);
	        }
	}

	public  static class MyReducer extends Reducer<Text,Text,Text,Text>{
		public  void reduce(Text key,Iterable<Text> value,Context context)throws IOException,InterruptedException{
			double sum=0;
			double sum_shang =0;
			double sum_count =0;
			double HE=2000;
			double gailv=0;
			for(Text val : value){
				String line = val.toString();
				String[] str =line.split(",")	;
				sum_shang += Double.parseDouble(str[0]);
				sum_count += Double.parseDouble(str[1]);
			}
			gailv=sum_count/HE;
			double shuxing_shang= -gailv*(Math.log(gailv)/Math.log(2));
			context.write(key,new Text(sum_shang+","+shuxing_shang));
			}
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		//设置MapReduce的输出的分隔符为逗号
		conf.set("mapred.textoutputformat.ignoreseparator","true");  
		conf.set("mapred.textoutputformat.separator",",");  
		Job job = new Job(conf,"PmiComput");
		job.setJarByClass(PMI_Comput.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://Master:9000/user/hadoop/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://Master:9000/user/hadoop/output"));
		System.exit(job.waitForCompletion(true)?0:1);
}
}
