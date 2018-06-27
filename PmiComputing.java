package PMI_Mapper;
// first 计算交叉熵
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class PmiComputing {
	

				public static class MyMapper extends Mapper<LongWritable,Text,Text,DoubleWritable>{
						   private DoubleWritable one = new DoubleWritable(1);
					        public void map(LongWritable key ,Text value,Context context)throws IOException, InterruptedException{
							String line = value.toString();
							String[] str =line.split(",")	;
							
							context.write(new Text(str[13]+","+str[41]),one); //str[i]为属性 ，str[41]为标签
					}
				}
				
				public  static class MyReduce extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{

					public void reduce(Text key,Iterable<DoubleWritable> value,Context context)throws IOException,InterruptedException{
						double sum=0;
						double gailv=0;
						double HE=2000; //窗口大小
						for(DoubleWritable val : value){
							sum+=val.get();
						}
						gailv=sum/HE;
						double shang= -gailv*(Math.log(gailv)/Math.log(2));//计算熵
						context.write(new Text(key+","+shang),new DoubleWritable(sum));
					}
			}			
				public static void main(String[] args)throws Exception{
					Configuration conf = new Configuration();
					//设置MapReduce的输出的分隔符为逗号
				    conf.set("mapred.textoutputformat.ignoreseparator","true");  
				    conf.set("mapred.textoutputformat.separator",",");  
				    Job job = new Job(conf,"PmiComputing");
				    job.setJarByClass(PmiComputing.class);
				    job.setMapperClass(MyMapper.class);
				    job.setReducerClass(MyReduce.class);
				    job.setOutputKeyClass(Text.class);
				    job.setOutputValueClass(DoubleWritable.class);
				    FileInputFormat.addInputPath(job, new Path("hdfs://Master:9000/user/hadoop/input"));
				    FileOutputFormat.setOutputPath(job, new Path("hdfs://Master:9000/user/hadoop/output"));
				    System.exit(job.waitForCompletion(true)?0:1);
	   }
}
				