package FileWriters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

/**
 * Created by ashu on 3/13/2017.
 */
public class Conf {
    private static Configuration configuration = new Configuration();

    public static Configuration getConf(){
        // set resource directories
        configuration.addResource(new Path("/home/ashu/hadoop/etc/hadoop/core-site.xml"));
        configuration.addResource(new Path("/home/ashu/hadoop/etc/hadoop/hdfs-site.xml"));

        configuration.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );

        configuration.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        // for java -jar command
        configuration.set("mapreduce.framework.name", "yarn");

        return configuration;
    }
}
