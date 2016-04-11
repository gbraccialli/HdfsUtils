package com.github.gbraccialli.hdfs;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class HDFSConfigUtils {

	private static final String[] HADOOP_CONF_FILES = {"core-site.xml", "hdfs-site.xml"};

	public static FileSystem loadConfigsAndGetFileSystem(String hadoopConfDirProp) throws Exception{
		return loadConfigsAndGetFileSystem(hadoopConfDirProp, null);
	}

	public static FileSystem loadConfigsAndGetFileSystem(String hadoopConfDirProp, String doAs) throws Exception{

		if (doAs != null){
			System.setProperty("HADOOP_USER_NAME", doAs);
		}
		
		Configuration config;
		FileSystem hdfs;
		if (hadoopConfDirProp == null)
			hadoopConfDirProp = "/etc/hadoop/conf";

		config = new Configuration(false);

		File hadoopConfDir = new File(hadoopConfDirProp).getAbsoluteFile();
		for (String file : HADOOP_CONF_FILES) {
			File f = new File(hadoopConfDir, file);
			if (f.exists()) {
				config.addResource(new Path(f.getAbsolutePath()));
			}
		}

		try {
			hdfs = FileSystem.get(config);
			if (doAs != null){
				hdfs = FileSystem.get(hdfs.getUri(),config, doAs);
			}
			return hdfs;

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		}


	}

}
