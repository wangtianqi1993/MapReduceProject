package JobControlTest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class JobControlTest {
	static String classtag = "1";
	//static final String INPUT_PATH = "/home/wtq/BigData-MachineLearning/Data/bupt-computation/log_newtrain.csv";
	static final String INPUT_PATH = "hdfs://localhost:9000/input/train.csv";
	//static final String OUTPUT_PATH = "hdfs://localhost:9000/output/busChampPart2/6lineBus/all/Weekend/" + day;
	static final String OUTPUT_PATH = "hdfs://localhost:9000/output/buptComputation/dealtrain/class" + classtag;
    static final String INPUT_PATH1 = "hdfs://localhost:9000/output/buptComputation/dealtrain/class1/part-r-00000";
	static final String OUTPUT_PATH1 = "hdfs://localhost:9000/output/buptComputation/dealtrain/jobcontrol";

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, URISyntaxException, InterruptedException {
		Configuration conf = new Configuration();
		Configuration conf1 = new Configuration();
		Path outpath = new Path(OUTPUT_PATH);
		Path outpath1 = new Path(OUTPUT_PATH1);
		FileSystem fileSystem = FileSystem.get(new URI(OUTPUT_PATH), conf);
		if (fileSystem.exists(outpath)) {
			fileSystem.delete(outpath, true);
		}
		org.apache.hadoop.mapreduce.Job job = new org.apache.hadoop.mapreduce.Job(conf, "BusSort");
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outpath);
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, INPUT_PATH);
		
		ControlledJob job1 = new ControlledJob(conf1);
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outpath1);
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, INPUT_PATH1);
		
		//设置依赖关系，构造DAG作业
		//JobConf extractJob = new JobConf(job.class);
		job.setJarByClass(JobControlTest.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		// job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job1.setJarByClass(JobControlTest.class);
		job1.setMapperClass(MyMapper1.class);
		job1.setReducerClass(Reducer.class);
		job1.setOutputKeyClass(LongWritable.class);
		job1.setOutputValueClass(Text.class);
//		try {
//			job.waitForCompletion(true);
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		job1.addDepending(job);
		JobControl JC = new JobControl("test");
		JC.addJob(job);
		JC.addJob(job1);
		JC.run();
		
	
		
		
        
		System.out.println("end of it!!!");
	}
    
	//整个map阶段会统计相同的key的value个数，并根据不同的key进行排序
	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private static LongWritable data = new LongWritable();
		long date;
		long calculate;
		String delDate;
		protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
			String[] splits = v1.toString().split(",");
			delDate = splits[7];
			date = Long.parseLong(delDate);
			data.set(date);     
            context.write(data, new Text(v1));
	}
	}
	static class MyReducer extends Reducer<LongWritable, Text, Text, Text> // map的输出为reduce的输入
	{
		private LongWritable data = new LongWritable();
		String sums;
		int count = 0;
        String[] date;
		// 对于每个map传过来的<k2,list{v2}>都会执行一次reduce函数,每一个key都会执行一次reduce
        @Override protected void reduce(LongWritable k2, java.lang.Iterable<Text> v2s,
				org.apache.hadoop.mapreduce.Reducer<LongWritable, Text, Text, Text>.Context context)
						throws IOException, InterruptedException {
			// data.set(0);
			long sum = 0;
			int bj = 0;
			for (Text value : v2s) {

				sum += 1;
				if(bj == 0)
				{
					date = value.toString().split(",");
					bj = 1;
				}
				
			}
			count++;
			sums = Long.toString(sum);
			System.out.println(sums);
			// 将该日期出现的总次数写入文件中
			context.write(new Text(date[7]), new Text(sums));
			System.out.println("counts->" + count);
		}
	}
	
	static class MyMapper1 extends Mapper<LongWritable, Text, LongWritable, Text>
	{
		long date;
		private static LongWritable data = new LongWritable();
		protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
			String[] splits = v1.toString().split("/t"); 
			date = Long.parseLong(splits[1]);
			data.set(date);
			context.write(data, new Text(splits[0]));
		}
	}
//	static class MyReducer1 extends Reducer<LongWritable, Text, Text, Text>
//	{
//		protected void reduce 
//	}
}

