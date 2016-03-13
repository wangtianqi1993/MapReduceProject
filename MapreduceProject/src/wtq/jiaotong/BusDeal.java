package wtq.jiaotong;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class BusDeal {
	
	static final String classtag = "4";
	//static final String INPUT_PATH = "hdfs://localhost:9000/output/busChampPart2/11lineBus/part-r-00000";
	//static final String OUTPUT_PATH = "hdfs://localhost:9000/output/busChampPart2/11lineBus/student";
	static final String INPUT_PATH = "tachyon://localhost:29999/output/busChampPart2/11lineBus/part-r-00000";
	//static final String INPUT_PATH = "hdfs://localhost:9000/input/wordCount/log_newtrain.csv";
	static final String OUTPUT_PATH = "hdfs://localhost:9000/output/buptComputation/dealtrain/class" + classtag;

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, URISyntaxException, InterruptedException {
		Configuration conf = new Configuration();
		Path outpath = new Path(OUTPUT_PATH);
		FileSystem fileSystem = FileSystem.get(new URI(OUTPUT_PATH), conf);
		if (fileSystem.exists(outpath)) {
			fileSystem.delete(outpath, true);
		}
		org.apache.hadoop.mapreduce.Job job = new org.apache.hadoop.mapreduce.Job(conf,"BusDeal");
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outpath);
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setJarByClass(BusDeal.class);
		job.setMapperClass(MyMapper.class);
	    job.setReducerClass(MyReducer.class);
		//job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		//map的输出默认与setOutputClass是相同的，当两者不相符时一定要注意分别定义类型
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		try {
			job.waitForCompletion(true);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		System.out.println("end of it!!!");
	}

	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private static LongWritable data = new LongWritable();
		protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
		  
			String[] splits = v1.toString().split(",");
			data.set(Long.parseLong(splits[4]));
			if(classtag.equals(splits[7])) {
				System.out.println(v1);
		    //在自定义MR时要保证Map在输出时的key为Longwritable（要是key有意义不能为空否则就无法执行）
			context.write(data, v1); // 一行的任务分解完成
			}
		}
	}
	static class MyReducer extends Reducer<LongWritable, Text, Text, Text> // map的输出为reduce的输入
	{
		//private LongWritable data = new LongWritable();
		protected void reduce(LongWritable k2, java.lang.Iterable<Text> v2s,
				org.apache.hadoop.mapreduce.Reducer<LongWritable,Text,Text, Text>.Context context)
						throws IOException, InterruptedException {
			// data.set(0);
			System.out.println("reduce");
			for (Text value : v2s) {
				context.write(null, new Text(value));
				break;
			}
		}
	}
}
