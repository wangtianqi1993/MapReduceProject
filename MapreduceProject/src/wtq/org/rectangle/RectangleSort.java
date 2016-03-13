package wtq.org.rectangle;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import wtq.org.rectangle.RectangleWritable.MyPatitioner;

//import com.sun.org.apache.xerces.internal.util.URI;

public class RectangleSort {

	static final String INPUT_PATH = "hdfs://localhost:9000/sort";
	static final String OUTPUT_PATH = "hdfs://localhost:9000/output";

	public static void main(String[] args)
			throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Path outpath = new Path(OUTPUT_PATH);
		FileSystem fileSystem = FileSystem.get(new URI(OUTPUT_PATH), conf);
		if (fileSystem.exists(outpath)) {
			fileSystem.delete(outpath, true);
		}
		Job job = new Job(conf, "RectangleSort");
		job.setJarByClass(RectangleWritable.class);
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class);
		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(RectangleWritable.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);

		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
		job.setOutputFormatClass(TextOutputFormat.class);
		//使用自定义分区
		job.setPartitionerClass(MyPatitioner.class);
		job.setNumReduceTasks(2);
		 
		job.waitForCompletion(true);
	}

	static class MyMapper extends Mapper<LongWritable, Text, RectangleWritable, NullWritable> {
		protected void map(LongWritable k1, Text v1,
				Mapper<LongWritable, Text, RectangleWritable, NullWritable>.Context context)
						throws IOException, InterruptedException {
			String[] splites = v1.toString().split(" ");
			RectangleWritable k2 = new RectangleWritable(Integer.parseInt(splites[0]), Integer.parseInt(splites[1]));
			context.write(k2, NullWritable.get());
		}
	}

	static class MyReducer extends Reducer<RectangleWritable, NullWritable, IntWritable, IntWritable> {
		@Override
		protected void reduce(RectangleWritable k2, Iterable<NullWritable> v2s,
				Reducer<RectangleWritable, NullWritable, IntWritable, IntWritable>.Context context)
						throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			context.write(new IntWritable(k2.getLength()), new IntWritable(k2.getLength()));
			// super.reduce(arg0, arg1, arg2);
		}
	}
}

class RectangleWritable implements WritableComparable {

	int length, width;

	public RectangleWritable(int length, int width) {
		super();
		this.length = length;
		this.width = width;
	}

	public RectangleWritable() {
		super();
		// TODO Auto-generated constructor stub
	}

	public int getLength() {
		return length;
	}

	public void setLength(int length) {
		this.length = length;
	}

	public int getWidth() {
		return width;
	}

	public void setWidth(int width) {
		this.width = width;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		out.writeInt(length);
		out.writeInt(width);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		this.length = in.readInt();
		this.width = in.readInt();
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		RectangleWritable to = (RectangleWritable) o;
		if (this.getLength() * this.getWidth() > to.getLength() * to.getWidth())
			return 1;
		if (this.getLength() * this.getWidth() < to.getLength() * to.getWidth())
			return -1;
		return 0;

	}
	
class MyPatitioner extends Partitioner<RectangleWritable,NullWritable>	
{

	@Override
	public int getPartition(RectangleWritable k2, 
			NullWritable v2, int numRecduceTasks) {
		// TODO Auto-generated method stub
		if (k2.getLength() == k2.getWidth()){
			return 0;
		} else
			return 1;
	}
	
}
	@Override
	public String toString() {
		return  length + "\t" +  width ;
	}
}