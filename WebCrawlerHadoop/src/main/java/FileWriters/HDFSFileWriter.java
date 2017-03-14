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

    private HDFSFileWriter() {

    }

    public synchronized void writeToFile(StringBuilder sb) throws IOException {
        String output = "output";

        Configuration conf = Conf.getConf();

        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
        }catch (IOException e){
            System.out.println("Can't create File System: " + e);
        }

        Path workingDir = fs.getHomeDirectory();
        Path out = new Path("/WebCrawler/iter0/" + output);

        Path completePath = Path.mergePaths(workingDir,out);

       BufferedWriter bw = null;

       try {
           if(fs.exists(completePath)){
               bw = new BufferedWriter(new OutputStreamWriter(fs.append(completePath)));
           } else bw = new BufferedWriter(new OutputStreamWriter(fs.create(completePath)));
       }catch (IOException e){
           e.printStackTrace();
       }

         if (sb != null) {
            bw.write(sb.toString());
            bw.newLine();
        }
        bw.close();
    }

    // return the singleton instance
    public static HDFSFileWriter getInstance(){
        return instance;
    }
}
