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
    private static String output;
    private static String[] resourcePaths;
    private static BufferedWriter bw;

    public static void setConf(String path, String[] resources){
        output = path;
        resourcePaths = resources;
    }

    private HDFSFileWriter(){
        Configuration conf = new Configuration();
        for(String resourcePath: resourcePaths){
            conf.addResource(new Path(resourcePath));
        }

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

        Path workingDir = fs.getWorkingDirectory();
        Path out = new Path("/WebCrawler/" + output + ".tsv");

        Path completePath = Path.mergePaths(workingDir,out);
        try {
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(completePath,true)));
        }catch (IOException e){
           System.out.println("Can't create writer: " + e);
        }
    }

    public synchronized void writeToFile(StringBuilder sb, boolean isShutDown) throws IOException {
        if(isShutDown) return;
        if (sb != null) {
            bw.write(sb.toString());
            bw.newLine();
        }
    }

    // return the singleton instance
    public static HDFSFileWriter getInstance(){
        return instance;
    }
}
