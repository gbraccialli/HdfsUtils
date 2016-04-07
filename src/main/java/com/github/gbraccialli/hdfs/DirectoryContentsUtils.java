package com.github.gbraccialli.hdfs;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DirectoryContentsUtils {

	public static void main(String[] args) throws Exception {

		String confDir = "/etc/hadoop/conf";
		String path = "/";
		int maxLevelThreshold = -1;
		long minSizeThreshold = -1;
		boolean showFiles = false;
		boolean verbose = false;
		
		for (String arg : args){
			if (arg.startsWith("--confDir=")){
				confDir = arg.split("=")[1];
			}
			if (arg.startsWith("--path=")){
				path = arg.split("=")[1];
			}
			if (arg.startsWith("--maxLevelThreshold=")){
				maxLevelThreshold = Integer.parseInt(arg.split("=")[1]);
			}
			if (arg.startsWith("--minSizeThreshold=")){
				minSizeThreshold = Long.parseLong(arg.split("=")[1]);
			}
			if (arg.startsWith("--showFiles=")){
				showFiles = Boolean.parseBoolean(arg.split("=")[1]);
			}
			if (arg.startsWith("--verbose=")){
				verbose = Boolean.parseBoolean(arg.split("=")[1]);
			}
		}

		Date dateStart = new Date();
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");

		FileSystem hdfs = HDFSConfigUtils.loadConfigsAndGetFileSystem(confDir);
		Path hdfsPath = new Path(path);

		if (verbose){
			System.err.println("confDir=" + confDir);
			System.err.println("path=" + path);
			System.err.println("maxLevelThreshold=" + maxLevelThreshold);
			System.err.println("minSizeThreshold=" + minSizeThreshold);
			System.err.println("showFiles=" + showFiles);
			System.err.println("verbose=" + verbose);
			System.err.println("uri=" + hdfs.getUri().toString());
			System.err.println("start at:" + dateFormat.format(dateStart));
		}

		System.out.println(directoryInfoToJson(DirectoryContentsUtils.listContents(hdfs,hdfsPath,0,maxLevelThreshold,minSizeThreshold,showFiles,verbose)));

		if (verbose){
			Date dateEnd = new Date();
			System.err.println();
			System.err.println("end at:" + dateFormat.format(dateEnd));
			System.err.println("elapsed time: " + (dateEnd.getTime() - dateStart.getTime()) / 1000.0 + " seconds") ;
		}
	}

	private static String directoryInfoToJson(PathInfo directoryInfo) {
		Gson gson = new GsonBuilder().create();
		return gson.toJson(directoryInfo);
	}

	public static PathInfo listContents(FileSystem hdfs, Path path) throws Exception{
		return listContents(hdfs,path,0,-1,-1,false,false);
	}

	public static PathInfo listContents(FileSystem hdfs, Path path, boolean verbose) throws Exception{
		return listContents(hdfs,path,0,-1,-1,false,verbose);
	}

	public static PathInfo listContents(FileSystem hdfs, Path path, int currentLevel, int maxLevelThreshold, long minSizeThreshold, boolean showFiles, boolean verbose) throws Exception{

		PathInfo dir = new PathInfo();
		ArrayList<PathInfo> children = new ArrayList<PathInfo>();

		ContentSummary summary = hdfs.getContentSummary(path);
		FileStatus fileStatus = hdfs.getFileStatus(path);

		long totalLength = summary.getLength();
		long totalSpaceConsumed = summary.getSpaceConsumed();
		dir.setName(path.getName());
		dir.setFullName(fileStatus.getPath().toUri().getPath());
		dir.setDirectory(fileStatus.isDirectory());
		dir.setLength(totalLength);
		dir.setSpaceConsumed(totalSpaceConsumed);
		dir.setNumberOfFiles(summary.getFileCount());
		dir.setNumberOfSubDirectories(summary.getDirectoryCount());

		if (verbose){
			System.err.println("Processing dir: " + dir.getFullName());
		}

		if (dir.isDirectory()){
			if (maxLevelThreshold > -1 && currentLevel >= maxLevelThreshold){
				dir.setMessage("Drill down stopped due to maxLevelThreshold achieved (" + maxLevelThreshold + ")");
				if (verbose){
					System.err.println("Drill down stopped due to maxLevelThreshold achieved (" + maxLevelThreshold + ")");
				}
			}else if (minSizeThreshold > -1 && totalLength < minSizeThreshold){
				dir.setMessage("Drill down stopped due to minSizeThreshold achieved (" + minSizeThreshold + ")");
				if (verbose){
					System.err.println("Drill down stopped due to minSizeThreshold achieved (" + minSizeThreshold + ")");
				}
			}else{
				long subDirsLength=0;
				long subDirsSpaceConsumed=0;
				long files=0;
				for (FileStatus fs : hdfs.listStatus(path)){
					if (fs.isDirectory()){
						PathInfo child = listContents(hdfs,fs.getPath(), currentLevel+1, maxLevelThreshold, minSizeThreshold, showFiles, verbose);
						children.add(child);
						subDirsLength += child.getLength();
						subDirsSpaceConsumed += child.getSpaceConsumed();
					}else if (showFiles){
						PathInfo child = listContents(hdfs,fs.getPath(), currentLevel+1, maxLevelThreshold, minSizeThreshold, showFiles, verbose);
						children.add(child);
						files++;
					}
				}
				if (!showFiles && files > 0){
					PathInfo multipleFilesInfo = new PathInfo("(" + files + " files)", hdfs.getFileStatus(path).getPath().toUri().getPath(),
							false,totalLength-subDirsLength,totalSpaceConsumed-subDirsSpaceConsumed,files, "multiple files entry to reduce visualization pressure");
					children.add(multipleFilesInfo);
				}
			}
			dir.setChildren(children);
		}
		return dir;
	}
}