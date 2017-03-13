package FileWriters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * Created by ashu on 3/11/2017.
 */

// writes a file to the HDFS
public class HDFSFileWriter  {

    private static final HDFSFileWriter instance = new HDFSFileWriter();
    // resources directory
    private static BufferedWriter bw;


    private HDFSFileWriter(){
        String output = "output";

        Configuration conf = Conf.getConf();

        // hadoop file system configurations
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        FileSystem fs = null;
        try {
             fs = FileSystem.get(conf);
        }catch (IOException e){
            System.out.println("Can't create File System: " + e);
        }

        Path workingDir = fs.getHomeDirectory();
        Path out = new Path("/WebCrawler/iter0/" + output + ".tsv");

        Path completePath = Path.mergePaths(workingDir,out);
        try {
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(completePath,true)));
        }catch (IOException e){
           System.out.println("Can't create writer: " + e);
        }
    }

    public synchronized void writeToFile(StringBuilder sb, boolean isShutDown) throws IOException {
        if(isShutDown) {
            bw.close();
        }
        else if (sb != null) {
            bw.write(sb.toString());
            bw.newLine();
        }
    }

    // return the singleton instance
    public static HDFSFileWriter getInstance(){
        return instance;
    }
}
