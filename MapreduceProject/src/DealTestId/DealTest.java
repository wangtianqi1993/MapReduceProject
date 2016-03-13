package DealTestId;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//处理test_item中的数据，将其中在bought_history不存在的，替换为相同类型确保在bougth_history中存在
public class DealTest {

	static final String PATH1 = "hdfs://localhost:9000/input/smallHistory.txt";
	static final String PATH2 = "hdfs://localhost:9000/input/test_items.txt";
	static final String PATH3 = "hdfs://localhost:9000/input/itemSortedByType.txt";
	static final String OUTPUT = "hdfs://localhost:9000/output/dealtest";
	static String[] testId = new String[6000];
	static int testLen = 0;
	static String[] itemsId = new String[501000];
	static int itemLen = 0;
	static String[] smallHistory = new String[500000];
	static int historyLength = 0;

	public static void main(String args[]) throws IOException, URISyntaxException {
		Configuration conf = new Configuration();
		Path outpath = new Path(OUTPUT);
		FileSystem fileSystem = FileSystem.get(new URI(OUTPUT), conf);
		if (fileSystem.exists(outpath)) {
			fileSystem.delete(outpath, true);
		}
		Path phistory = new Path(PATH1);
		Path ptest = new Path(PATH2);
		Path pdim = new Path(PATH3);
		BufferedReader br1 = new BufferedReader(new InputStreamReader(fileSystem.open(ptest)));
		try {
			String lines = br1.readLine();
			while (lines != null) {
				testId[testLen] = lines;
				lines = br1.readLine();
				testLen += 1;
			}
		} finally {
			br1.close();
		}

		BufferedReader br2 = new BufferedReader(new InputStreamReader(fileSystem.open(pdim)));
		try {
			String lines = br2.readLine();
			while (lines != null) {
				itemsId[itemLen] = lines;
				lines = br2.readLine();
				itemLen += 1;
			}
		} finally {
			br1.close();
		}

		BufferedReader br3 = new BufferedReader(new InputStreamReader(fileSystem.open(phistory)));
		try {
			String lines = br3.readLine();
			while (lines != null) {
				smallHistory[historyLength] = lines;
				lines = br3.readLine();
				historyLength += 1;
			}
		} finally {
			br1.close();
		}
       
		dealTest(); //执行处理函数
		System.out.println("end of main!!!");
		
	}

	static public boolean isInHistory(String test) {
		int bj = 0;
		for (int i = 0; i < historyLength; i++) {
			if (test.equals(smallHistory[i].split(" ")[1])) {
				bj = 1;
				break;
			}
		}
		if (bj == 1)
			return false;
		else
			return true;
	}

	static public void dealTest() {

		String[] seamType = new String[500000];
		int seamLen = 0;
		String type = null;
		int find = 0;
		for (int i = 0; i < testLen; i++) // 依次将dim_test中的id到history中检测处理
		{
			find = 0;
			seamLen = 0;
			if (isInHistory(testId[i])) // 不在history中则对该元素进行替换
			{
				for (int j = 0; j < itemLen; j++) {
					if (testId[i].equals(itemsId[j].split(" ")[0])) {
						type = itemsId[j].split(" ")[1];
						break;
					}
				}
				for (int m = 0; m < itemLen; m++) {
					//System.out.println("itemsId->" + itemsId[m]);
					String[] obj = itemsId[m].split(" ");
					if (type.equals(obj[1])) {
						seamType[seamLen++] = obj[0];
						find = 1;
					} else {
						if (find == 1)
							break;
					}
				}
				for (int k = 0; k < seamLen; k++) {
					if (!isInHistory(seamType[k])) {
						testId[i] = seamType[k];
						break;
					}

				}

			}
           System.out.println(testId[i]);
		}
	}
}
