import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;

/**
 * Created by ashu on 3/11/2017.
 */

// writes a file to the HDFS
public class HDFSFileWriter  {


   // change back to buffered writer
    public static PrintWriter get(String output, String[] resourcePaths) throws IOException{

        Configuration conf = new Configuration();
        for(String resourcePath: resourcePaths){
            conf.addResource(new Path(resourcePath));
        }

        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        FileSystem fs = FileSystem.get(conf);
        Path workingDir = fs.getWorkingDirectory();
        Path out = new Path("/WebCrawler/" + output + ".tsv");

        Path completePath = Path.mergePaths(workingDir,out);
        System.out.print(completePath.toString());

        //return new BufferedWriter(new OutputStreamWriter(fs.create(completePath)));
        return new PrintWriter(new OutputStreamWriter(fs.create(completePath)));
    }

}
