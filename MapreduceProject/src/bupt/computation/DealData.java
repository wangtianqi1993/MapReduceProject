package bupt.computation;

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

public class DealData {

	static final String classtag = "4";
	//static final String INPUT_PATH = "hdfs://localhost:9000/input/wordCount/log_newtrain.csv";
	// static final String INPUT_PATH =
	// "hdfs://localhost:9000/output/buptComputation/dealtrain/class" +
	// classtag;
	static final String INPUT_PATH = "tachyon://localhost:19998/input/test/part-r-00000";
	//static final String INPUT_PATH = "hdfs://localhost:9000/output/busChampPart2/6lineBus/part-r-00000";
	static final String OUTPUT_PATH = "hdfs://localhost:9000/output/buptComputation/dealtrain/class" + classtag;

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, URISyntaxException, InterruptedException {
		long a = System.currentTimeMillis();
		Configuration conf = new Configuration();
		Path outpath = new Path(OUTPUT_PATH);
		FileSystem fileSystem = FileSystem.get(new URI(OUTPUT_PATH), conf);
		if (fileSystem.exists(outpath)) {
			fileSystem.delete(outpath, true);
		}
		org.apache.hadoop.mapreduce.Job job = new org.apache.hadoop.mapreduce.Job(conf, "DealData");
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outpath);
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setJarByClass(DealData.class);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
	    //job.setReducerClass(Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		try {
			job.waitForCompletion(true);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        System.out.println("\r<br>执行耗时 : "+(System.currentTimeMillis()-a)/1000f+" 秒 ");
		System.out.println("end of it!!!");
	}

	// 整个map阶段会统计相同的key的value个数，并根据不同的key进行排序
	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
		private static LongWritable data = new LongWritable();
		long date;
		long calculate;
		String delDate;

	     @Override	protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
			String[] splits = v1.toString().split(",");
			delDate = splits[5];
			date = Long.parseLong(delDate);
			data.set(date);
			// if("线路10".equals(splits[1])) {
			// 在自定义MR时要保证Map在输出时的key为Longwritable（要是key有意义不能为空否则就无法执行）
		 if(splits[5].endsWith("07"))
		   {
			//if (classtag.equals(splits[7])) {
				// System.out.println(v1);
				context.write(data, new Text(v1));
			}
		}
	}

	static class MyReducer extends Reducer<LongWritable, Text, Text, Text> // map的输出为reduce的输入
	{
       private LongWritable data = new LongWritable();
		String sums;
		int count = 0;
        String[] date;
		// 对于每个map传过来的<k2,list{v2}>都会执行一次reduce函数,每一个key都会执行一次reduce
	     @Override 	protected void reduce(LongWritable k2, java.lang.Iterable<Text> v2s,
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
					// context.write(null, new Text(value));
			}
			count++;
			sums = Long.toString(sum);
		//	System.out.println(sums);
			// 将该日期出现的总次数写入文件中
			context.write(new Text(date[5]), new Text(sums));
		//	System.out.println("counts->" + count);
		}
	}
}
