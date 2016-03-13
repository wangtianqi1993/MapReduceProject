package wtq.org.useridentify;

import java.io.IOException;
//import java.nio.file.Path;
//import java.net.URI;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
//import org.apache.hadoop.mapreduce.v2.app.job.Job;

public class LiuLiangCount {

	static final String INPUT_PATH = "hdfs://localhost:9000/input";
	static final String OUTPUT_PATH = "hdfs://localhost:9000/output";

	public static void main(String[] args)
			throws IOException, ClassNotFoundException, URISyntaxException, InterruptedException {
		Configuration conf = new Configuration();
		Path outpath = new Path(OUTPUT_PATH);
		FileSystem fileSystem = FileSystem.get(new URI(OUTPUT_PATH), conf);
		if (fileSystem.exists(outpath)) {
			fileSystem.delete(outpath, true);
		}
		org.apache.hadoop.mapreduce.Job job = new org.apache.hadoop.mapreduce.Job(conf, "LiuLiangCount");
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outpath);
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LiuLiangWritable.class);
		try {
			job.waitForCompletion(true);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	static class MyMapper extends Mapper<LongWritable, Text, Text, LiuLiangWritable> {
		protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
			String[] splits = v1.toString().split(" ");
			LiuLiangWritable v2LiuLiangWritable = new LiuLiangWritable(splits[6], splits[7], splits[8], splits[9]); // map输出的value
			Text k2 = new Text(splits[1]); // map输出的key
			context.write(k2, v2LiuLiangWritable); // 一行的任务分解完成
		}
	}

	static class MyReducer extends Reducer<Text, LiuLiangWritable, Text, LiuLiangWritable> // map的输出为reduce的输入
	{
		protected void reduce(Text k2, java.lang.Iterable<LiuLiangWritable> v2s,
				org.apache.hadoop.mapreduce.Reducer<Text, LiuLiangWritable, Text, LiuLiangWritable>.Context context)
						throws IOException, InterruptedException {
			long upPackNum = 0L, downPackNum = 0L, upPayLoad = 0L, downPayLoad = 0L;
			for (LiuLiangWritable liuLiangWritable : v2s) {
				upPackNum += liuLiangWritable.upPackNum;
				downPackNum += liuLiangWritable.downPackNum;
				upPayLoad += liuLiangWritable.upPayLoad;
				downPayLoad += liuLiangWritable.downPayLoad;
			}
			LiuLiangWritable v3 = new LiuLiangWritable(upPackNum + "", downPackNum + "", upPayLoad + "",
					downPayLoad + ""); // 通过加上 +“ ” 将Long类型转换为String
										// 来符合构造函数的类型要求
			context.write(k2, v3); // 将reduce的输出数据写到hdfs中
		};
	}
}
