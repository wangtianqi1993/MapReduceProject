package wtq.org;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
//import com.sun.org.apache.xerces.internal.util.URI;
import java.net.URISyntaxException;

public class CopyFile {
	public static void main(String[] args) throws Exception
	{
	    readFromServer();  
	   // putFileToServer() ;
	   //  createDir();
	    //removeDir();
	    listDirOrFile();
	}
	private static void readFromServer() throws URISyntaxException, IOException {
		URI uri = new URI("hdfs://localhost:9000/");
		FileSystem fileSystem = FileSystem.get(uri, new Configuration());
		FSDataInputStream fSDataInputStream = fileSystem.open(new Path("hdfs://localhost:9000/update.txt"));
        IOUtils.copyBytes(fSDataInputStream, System.out, new Configuration(),false);
	}
	private static void putFileToServer() throws URISyntaxException,
	                IOException{
		   URI uri = new URI("hdfs://localhost:9000/");
		   FileSystem fileSystem = FileSystem.get(uri, new Configuration());
		  FileInputStream fileInputStream = new FileInputStream("update.txt");
		   FSDataOutputStream fSDataOutputStream = fileSystem.create(new Path("hdfs://localhost:9000/update.txt") );
		  IOUtils.copyBytes(fileInputStream, fSDataOutputStream, new Configuration(),true);
	}
	private static void createDir() throws URISyntaxException,IOException {
		    URI uri = new URI("hdfs://localhost:9000/");
		    FileSystem fileSystem = FileSystem.get(uri, new Configuration());
		    fileSystem.mkdirs(new Path("hdfs://localhost:9000/newdir"));
	}
	private static void removeDir() throws URISyntaxException,IOException
	{
		URI uri = new URI("hdfs://localhost:9000/");
		FileSystem fileSystem = FileSystem.get(uri, new Configuration());
		fileSystem.delete(new Path("hdfs://localhost:9000/newdir"),true);
	}
	//列出目录或文件列表
	 private static void listDirOrFile() throws URISyntaxException,IOException
	 {
		 URI uri = new URI("hdfs://localhost:9000/");
		 FileSystem fileSystem = FileSystem.get(uri, new Configuration());
		 FileStatus[] fileStatuses = fileSystem.listStatus(new Path("hdfs://localhost:9000/"));
		 for (FileStatus fileStatus : fileStatuses){
			 String type = fileStatus.isDir() ? "目录" : "文件";
			 String name = fileStatus.getPath().getName();
			 System.out.println(type + "\t" + name);
		 }
	 }
}
