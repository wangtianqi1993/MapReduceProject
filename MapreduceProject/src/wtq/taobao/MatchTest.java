package wtq.taobao;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
//import java.nio.file.Path;
//import java.net.URI;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.StringTokenizer;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.v2.app.job.Job;
//import org.apache.hadoop.mapreduce.v2.app.job.Job；

//import wtq.org.WordCount.MyMapper;
//import wtq.org.WordCount.MyReducer;

public class MatchTest {

	static String[] dimFashion = new String[30000]; // 存储读出的dim_fashion中的数据
	static int dimFashionLength = 0;
	static String[] userArray = new String[50000];
	static String[] shoppingId = new String[30000];
	static StringBuffer submit = null; // 存储每个test所匹配的商品列表
//	static StringBuffer buf = new StringBuffer();
    static String uId = null;
	static int timeSum = 0;
	static final String INPUT_PATH = "hdfs://localhost:9000/input/user_bought_history.txt";
	static final String OUTPUT_PATH = "hdfs://localhost:9000/output";
    static int distance;
    static String tempShop = null;
	public static void main(String[] args) throws IOException, ClassNotFoundException, URISyntaxException {

		FileOutputStream out = new FileOutputStream("/home/wtq/fm_submissions.txt");
		OutputStreamWriter osw = new OutputStreamWriter(out, "utf-8");
		BufferedWriter bwr = new BufferedWriter(osw);

		Configuration conf = new Configuration();
		Path outpath = new Path(OUTPUT_PATH);
		FileSystem fileSystem = FileSystem.get(new URI(OUTPUT_PATH), conf);
		if (fileSystem.exists(outpath)) {
			fileSystem.delete(outpath, true);
		}
		Path pt = new Path(INPUT_PATH);
		Path ptMatch = new Path("hdfs://localhost:9000/input/dim_fashion_matchsets.txt");
		// timeSum = 0;
		Path pathSort = new Path("hdfs://localhost:9000/input/afterSortHistory.txt");// afterSortHistory中为将dim_history按userId排序
		Path pathTest = new Path("hdfs://localhost:9000/input/sxData/sxTestItems.txt");
		Path pathNewTest = new Path("hdfs://localhost:9000/input/sxData/newSxData.txt");
		// 从fashion_match中读数据存到dimFashion中
		
		org.apache.hadoop.mapreduce.Job job = new org.apache.hadoop.mapreduce.Job(conf, "WordCount");
		org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job, outpath);
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.setInputPaths(job, INPUT_PATH);
		
		BufferedReader br2 = new BufferedReader(new InputStreamReader(fileSystem.open(ptMatch)));
		try {
			String lines = br2.readLine();
			while (lines != null) {
				dimFashion[dimFashionLength] = lines;
				lines = br2.readLine();
				dimFashionLength += 1;
			}
		} finally {
			br2.close();
		}
		int bjj = 0;
		String lineNew = null;
		String lineTest = null;
		BufferedReader br3 = new BufferedReader(new InputStreamReader(fileSystem.open(pathNewTest)));
		BufferedReader br4 = new BufferedReader(new InputStreamReader(fileSystem.open(pathTest)));
		lineTest = br4.readLine();
		lineNew = br3.readLine();
		try {
			lineNew = br3.readLine();
			while (lineNew != null) {
				//int shoppingLen = 0;
				
	     		userArray = getUserId(pathSort, fileSystem, lineNew);
				int userLen = 0;
			      int shopLen = 0;

				while(userArray[userLen] != null)
				{
				//shoppingId = getShoppingId(pathSort, fileSystem, timeSum, userArray);
				      uId = userArray[userLen];
				      userLen += 1;
				      distance = 9000;
				      job.setMapperClass(MyMapper.class);
						job.setReducerClass(MyReducer.class);
						job.setOutputKeyClass(LongWritable.class);
						job.setOutputValueClass(Text.class);
						try {
							job.waitForCompletion(true);
						} catch (InterruptedException e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
						shoppingId[shopLen] = tempShop;
						//！！！！要使每个test匹配的商品数目达到200，可再多取几个temshopId中的商品放到shopId中
						System.out.println("shopID-> " + shoppingId[shopLen] + " shopTh->" + shopLen);
						shopLen += 1;
				}
				submit = getMatchShoppingId(ptMatch, fileSystem, shoppingId);
				String contentString = submit.toString();
				// if(contentString.contains(";"))
				contentString = contentString.replaceAll(";", ",");
				contentString = removesamestring(contentString);
				contentString = lineTest + " " + contentString + "\r\n";
				bwr.write(contentString);
				// bwr.write('\n');
				lineNew = br3.readLine();
				lineTest = br4.readLine();
				bjj++;
				System.out.print("Count-> ");
				System.out.println(bjj);
			}
		} finally {
			br3.close();
			br4.close();
		}
		bwr.flush();
		bwr.close();
	
		System.out.println("end of main!!!");
	}

	public static String[] getUserId(Path pt, FileSystem fileSystem, String testId) throws IOException {
		timeSum = 0;
	    String[] tempArray = new String[50000];
		BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(pt)));
		try {
			String line;
			// 按行读取hdfs中一行的内容到line中
			line = br.readLine();
			int i = 0;
			while (line != null) {
				// 按照空格对每行数据进行分割并存到字符串数组中
				String[] splits = line.split(" ");
				// System.out.println(splits[0]);
				if (testId.equals(splits[1])) {
					// 防止多次购买同一商品的用户Id多次写入tempArray中
					tempArray[i++] = splits[0];
					if(i == 1000)
						break;
					// userTime[i] = splits[2];
					// 截取时间的后4位并转化为int类型
					timeSum += Integer.parseInt(splits[2].substring(4));
					System.out.println(tempArray[i - 1]);
				}
				line = br.readLine();
			}
				timeSum = timeSum / i;
				System.out.println("sumtime -> " + timeSum);
				System.out.print("UserId Length->");
				System.out.println(i);
			
		} finally {
			br.close();
		}
		return tempArray;
	}

	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

		protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
			String[] splits = v1.toString().split(" ");
		//String tempShop = null;
			//int shopLen = 0;
			System.out.println("map uId->" + uId);
			if (uId.equals(splits[0])) {
				int dis = Math.abs(Integer.parseInt(splits[2].substring(4)) - timeSum);
				if (dis < distance) {
					distance = dis;
				    tempShop = splits[1];
					//tempshopId[j++] = splits[1];  //可以换成String tempShop = splits[1] 
				}
					}
		//String values = splits[0] + " " + splits[1] + " " + splits[2];
			//将data作为key值传入下一过程，接下来将自动按照key的大小进行排序
			context.write(k1, new Text(v1));
		}
	}
    static class MyReducer extends Reducer<LongWritable,Text,Text,Text>
    {
		protected void reduce(LongWritable k2, java.lang.Iterable<Text> v2s,
				org.apache.hadoop.mapreduce.Reducer<LongWritable, Text,Text,Text>.Context context)
						throws IOException, InterruptedException {
			for (Text value : v2s) {
				context.write(null,new Text(value));
				break;
			}
			//context.write(null, new Text(v2s[0]));
		};
	}
	
	// 根据userId与time到history中搜索在time时所购买的商品所有商品存到一个String数组即可
	/*public static String[] getShoppingId(Path pt, FileSystem fileSystem, int time, String[] userId) throws IOException {
		String[] shopId = new String[10000];
		//String[] tempshopId = new String[1000];
		//int i = 0;
		int j = 0;
		int k = 0;
		int distence;
		int tempShopId = 0;
		int finded; // 用来标记该userId是否被查找过
		while (userId[k] != null) {
			BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(pt)));
			try {
				// 按行读取hdfs中一行的内容到line中
				String tempShop = null;
				distence = 9000;
				j = 0;
				finded = 0;
				String line = br.readLine();    //String line 的定义放到此处更高效
				while (line != null) {
					// 以空格为单位拆分每行，拆的元素存储到splits中
					String splits[] = line.split(" ");
					if (userId[k].equals(splits[0])) {
						int dis = Math.abs(Integer.parseInt(splits[2].substring(4)) - time);
						if (dis < distence) {
							distence = dis;
						    tempShop = splits[1];
							//tempshopId[j++] = splits[1];  //可以换成String tempShop = splits[1] 
						}
						finded = 1;
					} else {
						if (finded == 1)
							break;
					}
					line = br.readLine();
				}
				shopId[k] = tempShop;
				//！！！！要使每个test匹配的商品数目达到200，可再多取几个temshopId中的商品放到shopId中
				System.out.println("shopID-> " + shopId[k] + " k->" + k);
				k++;
			} finally {
				br.close();
			}
		}
		return shopId;
	}*/
	

	// 传进的与test相匹配的字符数组，到fashionmatch中进行搜索找到匹配的商品
	public static StringBuffer getMatchShoppingId(Path pt, FileSystem fileSystem, String[] shoppingId)
			throws IOException {

		int k = 0;
		int j = 0;
		int stringBj = 0;
		int i;
		int bj;
		int bj1;
		int shopLength = 0;
		for (int l = 0; l < shoppingId.length; l++) {
			if (shoppingId[l] != null)
				shopLength++;
		}
		// String[] splits1 = new String[5000];
		// String[] splits2 = new String[5000];
		// String[] splits3 = new String[5000];
		String splits4 = null;
		// String tempMatchId = null;
		StringBuffer buf = new StringBuffer();
		// System.out.println("ShoppingId length is ->" + shoppingId.length);
		BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(pt)));
		try {
			String line = br.readLine(); 
			while (line != null) {
				k = 0;
				j = 0;
				bj1 = 0;
				stringBj = 0;
				int hasdouhao = 0;
				String[] splits1 = line.split(" ");
				// 对splits1［1］判断是否包含，
				String[] splits2 = splits1[1].split(";");

				for (stringBj = 0; stringBj < splits2.length; stringBj++) {
					bj1 = 0;
					// 要判断字符串中有无，才能用，分割
					if (splits2[stringBj].contains(",")) {
						String[] splits3 = splits2[stringBj].split(",");
						for (k = 0; k < shopLength; k++) {
							bj = 0;
							for (j = 0; j < splits3.length; j++) {
								// System.out.println("splits3->" + splits3[j]);
								if (shoppingId[k].equals(splits3[j])) {
									bj = 1;
									break;
								}
							}
							if (bj == 1) {
								buf.append(splits1[1]);
								buf.append(",");
								bj1 = 1;
								break;
							}
						}
					} else {
						splits4 = splits2[stringBj];
						// System.out.println("splits4->"+splits4);
						// 分割出的一行匹配ID依次与shoppID进行匹配
						for (k = 0; k < shopLength; k++) {
							bj = 0;
						//		System.out.println("shoppingId->" + shoppingId[k] + "splits->" + splits4);
							if (shoppingId[k].equals(splits4)) {
								bj = 1;
								if (bj == 1) {
									buf.append(splits1[1]);
									buf.append(",");
									bj1 = 1;
									break;
								}
							}
						}

					}
					// 依次读取fashion—match中的每行逐一与shopID中的每个shopId相匹配，一旦匹配成功
					// 就将match表中整个一行写入到buf中，并break出当前循环，在match中读取下一条记录重复上面动作
					if (bj1 == 1)
						break;
				}
				line = br.readLine();
			}
		} finally {
			br.close();
		}
		System.out.println("MatchId-> " + buf);
		return buf;
	}

	// 去除字符串中的重复字段
	private static String removesamestring(String str) {
		Set<String> mlinkedset = new LinkedHashSet<String>();
		String[] strarray = str.split(",");
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < strarray.length; i++) {
			if (!mlinkedset.contains(strarray[i])) {
				mlinkedset.add(strarray[i]);
				sb.append(strarray[i] + ",");
			}
		}
		// System.out.println(mlinkedset);
		return sb.toString().substring(0, sb.toString().length() - 1);
	}

}
