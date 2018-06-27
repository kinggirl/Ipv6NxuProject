package PMI_Mapper;
//third
//计算互信息
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class PMI_comput_end {
	public static class MyMapper extends Mapper<LongWritable,Text,Text, Text>{
		private Text word =new Text();
	        public void map(LongWritable key ,Text value,Context context)throws IOException, InterruptedException{
			String line = value.toString();
			String[] str =line.split(",")	;
			word.set(str[1]+","+str[2]);
			context.write(new Text("V9"),word);
	        }
	}
	public  static class MyReducer extends Reducer<Text,Text,Text,DoubleWritable>{

		public  void reduce(Text key,Iterable<Text> value,Context context)throws IOException,InterruptedException{
			double x_shang=0;
			double y_x_shang=0;
			double pmi=0;
			for(Text  val : value){
				String line = val.toString();
				String[] str =line.split(",")	;
				 y_x_shang += Double.parseDouble(str[0]);
				 x_shang += Double.parseDouble(str[1]);
			}
			pmi= x_shang - y_x_shang;//计算互信息
			context.write(new Text("V41"),new DoubleWritable(pmi));//  value为该属性互信息			
			}
	}
	public static void main(String[] args)throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf,"PmiComputend");
		job.setJarByClass(PMI_comput_end.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://Master:9000/user/hadoop/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://Master:9000/user/hadoop/output"));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
