package tachyon_test;
import java.io.IOException;

import tachyon.TachyonURI;
import tachyon.client.FileOutStream;
import tachyon.client.InStream;
import tachyon.client.OutStream;
import tachyon.client.ReadType;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.WriteType;
import tachyon.util.CommonUtils;

public class TachyonTest {
	 public final static TachyonURI masteruri = new TachyonURI("tachyon://localhost:19998");
	    public final static TachyonURI filepath = new TachyonURI("/tmp/test");
	    public final static WriteType writeType = WriteType.CACHE_THROUGH;
	    public final static ReadType readType = ReadType.CACHE;
	 
	    public static void writeFile() throws IOException
	    {
	        TachyonFS tachyonClient = TachyonFS.get(masteruri);
	        if(tachyonClient.exist(filepath)) {
	            tachyonClient.delete(filepath, true);
	        }
	        tachyonClient.createFile(filepath);
	        TachyonFile file = tachyonClient.getFile(filepath);
	        FileOutStream os = (FileOutStream) file.getOutStream(writeType);
	        for(int i = 0; i < 10; i ++)
	        {
	            os.write(Integer.toString(i).getBytes());
	        }
	        os.close();
	        tachyonClient.close();
	    }
	 
	    public static void readFile() throws IOException
	    {
	        TachyonFS tachyonClient = TachyonFS.get(masteruri);
	        TachyonFile file = tachyonClient.getFile(filepath);
	        InStream in = file.getInStream(readType);
	        byte[] bytes = new byte[20];
	        in.read(bytes);
	        System.out.println(new String(bytes));
	        in.close();
	        tachyonClient.close();
	    }
	     
	    public static void main(String[] args) throws IOException
	    {
	        writeFile();
	        readFile();
	        System.out.println("end!");
	    }
}
