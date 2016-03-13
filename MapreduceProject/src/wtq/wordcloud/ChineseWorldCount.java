package wtq.wordcloud;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class ChineseWorldCount {
	
	static final String INPUT_PATH = "/home/wtq/BigData-MachineLearning/Data/bupt-computation/worldcound.txt";
    static final String OUTPUT_PATH = "/home/wtq/BigData-MachineLearning/Data/bupt-computation/worldcoundouts.txt";
	//static final String INPUT_PATH = "hdfs://localhost:9000/input/bjtu.txt";
	//static final String OUTPUT_PATH = "hdfs://localhost:9000/output/ChineseWord/bjtu";
	
	public static class TokenizerMapper extends Mapper<Object,Text,Text,IntWritable>{
	     private final static IntWritable one = new IntWritable(1);
	     private Text word = new Text();
	     public void map(Object key,Text value, Context context) throws IOException,InterruptedException {
	    	 byte[] bt = value.getBytes();
	    	 InputStream ip = new ByteArrayInputStream(bt);
	    	 Reader read = new InputStreamReader(ip);
	         //使用IKAnalyzer的分词功能
	    	 IKSegmenter iks = new IKSegmenter(read,true);
	    	 Lexeme t;
	    	 while ((t = iks.next()) != null)
	    	 {
	    		 word.set(t.getLexemeText());
	    		 //System.out.println(word);
	    		 context.write(word, one);
	    	 }
	     }
	     
	}
	public static class IntSumReader extends Reducer<Text,IntWritable,Text,IntWritable>{
		private IntWritable result = new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values,org.apache.hadoop.mapreduce.Reducer<Text,IntWritable,Text,IntWritable>.Context context) 
				throws IOException,InterruptedException{
			int sum = 0;
			for(IntWritable val : values){
				sum += val.get();
			}
			System.out.println(key);
			System.out.println(sum);
			result.set(sum);
			context.write(key,result);
		}
	}
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		Path outpath = new Path(OUTPUT_PATH);
		FileSystem fileSystem = FileSystem.get(new URI(OUTPUT_PATH), conf);
		if (fileSystem.exists(outpath)) {
			fileSystem.delete(outpath, true);
		}
		
		Job job = new Job(conf,"ChineseWorldCount");
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outpath);
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setJarByClass(ChineseWorldCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReader.class);
		job.setReducerClass(IntSumReader.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		try {
			job.waitForCompletion(true);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//FileInputFormat.addInputPath(job,new Path(otherArgs[0]));
		//FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		//System.exit(job.waitForCompletion(true)? 0 : 1);
		System.out.println("end of it!!!");
	}
}
