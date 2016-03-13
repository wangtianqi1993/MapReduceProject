package wtq.org.useridentify;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class LiuLiangWritable implements Writable {

	long upPackNum, downPackNum, upPayLoad, downPayLoad;
	 
	public LiuLiangWritable() {
		super();
		// TODO Auto-generated constructor stub
	}

	public LiuLiangWritable(String upPackNum, String downPackNum, String upPayLoad, String downPayLoad) {
		super();
		this.upPackNum = Long.parseLong(upPackNum);  //通过包装类将String类型转换为Long类型
		this.downPackNum = Long.parseLong(downPackNum);
		this.upPayLoad = Long.parseLong(upPayLoad);
		this.downPayLoad = Long.parseLong(downPayLoad);
	}

	//序列化
	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
              out.writeLong(upPackNum);
              out.writeLong(downPackNum);
              out.writeLong(upPayLoad);
              out.writeLong(downPayLoad);
 	}
	//反序列化
		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
				this.upPackNum = in.readLong();
				this.downPackNum = in.readLong();
				this.upPayLoad = in.readLong();
				this.downPayLoad = in.readLong();
		}
	@Override
	public String toString() {
		return  upPackNum + "\t" +  downPackNum + "\t " + upPayLoad +
				"\t" +  downPayLoad ;
	}


}
