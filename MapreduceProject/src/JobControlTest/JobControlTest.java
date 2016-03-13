package JobControlTest;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
//import java.nio.file.FileSystem;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class JobControlTest {
	
	 /***
	   *
	   *MapReduce作业1的Mapper
	   *
	   *LongWritable 1  代表输入的key值，默认是文本的位置偏移量
	   *Text 2          每行的具体内容
	   *Text 3          输出的Key类型
	   *Text 4          输出的Value类型
	   * 
	   * */
	
    static final String OUTPUT_PATH = "hdfs://localhost:9000/output/jobcontrol1";

	private static class SumMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text t = new Text();
		private IntWritable one = new IntWritable(1);
		/***
		 * map阶段输出词频
		 */
		protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
			String data = value.toString();
			String words[] = data.split(";");
			if(words[0].trim()!=null){
				t.set(" " + words[0]);
				one.set(Integer.parseInt(words[1]));
				context.write(t, one);
			}
		}
		
	}
	
	/**
	 * mapreduce作业1的reducer
	 * 词频累加并输出
	 */
	private static class SumReduce extends Reducer<Text, IntWritable, IntWritable, Text>
	{
		private IntWritable iw = new IntWritable();
		@Override
		protected void reduce(Text key, Iterable<IntWritable> value, Context context)
			throws IOException, InterruptedException{
			int sum = 0;
			for (IntWritable count:value){
				sum += count.get();
			}
			iw.set(sum);
			context.write(iw, key);
	
		}
	}
	
	/**
	 * mapreduce 作业2排序的mapper
	 */
	private static class SortMapper extends Mapper<LongWritable, Text, IntWritable, Text>
	{
		IntWritable iw = new IntWritable();//存储词频
		private Text t = new Text();//存储文本
		@Override
		protected void map(LongWritable key, Text value, Context context)throws IOException,
			InterruptedException {
			
			String words[] = value.toString().split(" ");
			System.out.println("length" + words.length);
			System.out.println("Map读入的文本" + value.toString());
			System.out.println("==>" + words[0]+ "==>" + words[1]);
			if (words[0]!=null){
				iw.set(Integer.parseInt(words[0].trim()));
				t.set(words[1].trim());
				context.write(iw, t); //map默认按key排序
			}
		}
	}
	/*
	 * mapreduce2之reducer
	 */
	private static class SortReduce extends Reducer<IntWritable, Text, Text, IntWritable>
	{
		/*
		 * 输出排序
		 */
		@Override
		protected void reduce(IntWritable key, Iterable<Text> value,Context context)
			throws IOException, InterruptedException{
			for(Text t:value){
				context.write(t, key);
			}
		}
	}
	
	 /***
	   * 排序组件，在排序作业中，需要使用
	   * 按key的降序排序
	   * 
	   * **/
    public static class DescSort extends WritableComparator{

       public DescSort() {
         super(IntWritable.class,true);//注册排序组件
      }
       @Override
      public int compare(byte[] arg0, int arg1, int arg2, byte[] arg3,
          int arg4, int arg5) {
        return	-super.compare(arg0, arg1, arg2, arg3, arg4, arg5);//注意使用负号来完成降序
      }
       
       @Override
      public int compare(Object a, Object b) {
     
        return	-super.compare(a, b);//注意使用负号来完成降序
      }
      
       /*
        * 驱动类
        */
       public static void main(String[] args)throws Exception{
    	   
    	   JobConf conf = new JobConf(JobControlTest.class);
    	   conf.set("mapred.job.tracker","10.108.115.114:9001");
    	   // conf.set("fs.default.name", "file:///");  
           //conf.setJar("tt.jar");
           System.out.println("模式：  "+conf.get("mapred.job.tracker"));;
           
           /*
            * 作用一配置统计词频
            */
           Job job1 = new Job(conf, "Join1");
           job1.setJarByClass(JobControlTest.class);
           job1.setMapperClass(SumMapper.class);
           job1.setReducerClass(SumReduce.class);
           
           job1.setMapOutputKeyClass(Text.class);//map阶段的输出的key
           job1.setMapOutputValueClass(IntWritable.class);//map阶段的输出的value
           
           job1.setOutputKeyClass(IntWritable.class);//reduce阶段的输出的key
           job1.setOutputValueClass(Text.class);//reduce阶段的输出的value
           
           //加入控制器
           ControlledJob ctrljob1 = new ControlledJob(conf);
           ctrljob1.setJob(job1);
           FileInputFormat.addInputPath(job1, new Path("hdfs://localhost:9000/input/jobcontrol-test/jobcontrol.txt"));
           //FileSystem fs = FileSystem.get(conf);
           //FileSystem fs = hdfsFile.getFileSystem(conf);
           Path op = new Path("hdfs://localhost:9000/output/jobcontrol1");
           FileSystem fs = FileSystem.get(new URI(OUTPUT_PATH), conf);
           if(fs.exists(op)){
        	   fs.delete(op, true);
        	   System.out.println("存在此输出路径，已删除！！！");  
           }
           FileOutputFormat.setOutputPath(job1, op);
    
           /**
            * 
            *作业2的配置
            *排序
            * 
            * **/
           Job job2=new Job(conf,"Join2");
           job2.setJarByClass(JobControlTest.class);
           //job2.setInputFormatClass(TextInputFormat.class);
             
           job2.setMapperClass(SortMapper.class);
           job2.setReducerClass(SortReduce.class);
           
           job2.setSortComparatorClass(DescSort.class);//按key降序排序
           
           job2.setMapOutputKeyClass(IntWritable.class);//map阶段的输出的key
           job2.setMapOutputValueClass(Text.class);//map阶段的输出的value
           
           job2.setOutputKeyClass(Text.class);//reduce阶段的输出的key
           job2.setOutputValueClass(IntWritable.class);//reduce阶段的输出的value
           ControlledJob ctrljob2 = new ControlledJob(conf);
           ctrljob2.setJob(job2);
           
           /***
            * 
            * 设置多个作业直接的依赖关系
            * 如下所写：
            * 意思为job2的启动，依赖于job1作业的完成
            * 
            * **/
           ctrljob2.addDependingJob(ctrljob1);
           
           
           //输入路径是上一个作业的输出路径
           FileInputFormat.addInputPath(job2, new Path("hdfs://localhost:9000/output/jobcontrol1/part*"));
           String OUT_PATH = "hdfs://localhost:9000/output/jobcontrol2";
           FileSystem fs2=FileSystem.get(new URI(OUT_PATH), conf);
            
           Path op2=new Path("hdfs://localhost:9000/output/jobcontrol2");
           if(fs2.exists(op2)){
              fs2.delete(op2, true);
              System.out.println("存在此输出路径，已删除！！！");
            }
           FileOutputFormat.setOutputPath(job2, op2);
           
           // System.exit(job2.waitForCompletion(true) ? 0 : 1);
           
           /**
            * 
            * 主的控制容器，控制上面的总的两个子作业
            * 
            * **/
           JobControl jobCtrl=new JobControl("myctrl");
           //ctrljob1.addDependingJob(ctrljob2);// job2在job1完成后，才可以启动
           //添加到总的JobControl里，进行控制
           
           jobCtrl.addJob(ctrljob1); 
           jobCtrl.addJob(ctrljob2);
           
        
           //在线程启动
           Thread  t=new Thread(jobCtrl);
           t.start();
           
           while(true){
             
             if(jobCtrl.allFinished()){//如果作业成功完成，就打印成功作业的信息
               System.out.println(jobCtrl.getSuccessfulJobList());
               
               jobCtrl.stop();
               break;
             }
             
             if(jobCtrl.getFailedJobList().size()>0){//如果作业失败，就打印失败作业的信息
               System.out.println(jobCtrl.getFailedJobList());
               
               jobCtrl.stop();
               break;
             }
             
           }
           
       }
    }
}


