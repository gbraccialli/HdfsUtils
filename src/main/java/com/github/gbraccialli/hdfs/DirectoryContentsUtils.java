package com.github.gbraccialli.hdfs;

import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class DirectoryContentsUtils {

	public static long countEntries = 0;

	public static void main(String[] args) throws Exception {


		String confDir = "/etc/hadoop/conf";
		String path = "/";
		int maxLevelThreshold = -1;
		long minSizeThreshold = -1;
		boolean showFiles = false;
		boolean verbose = false;
		String doAs = null;
		List<String> excludeList = null;

		for (String arg : args){
			if (arg.startsWith("--confDir=")){
				confDir = arg.split("=")[1];
			}else if (arg.startsWith("--path=")){
				path = arg.split("=")[1];
			}else if (arg.startsWith("--maxLevelThreshold=")){
				maxLevelThreshold = Integer.parseInt(arg.split("=")[1]);
			}else if (arg.startsWith("--minSizeThreshold=")){
				minSizeThreshold = Long.parseLong(arg.split("=")[1]);
			}else if (arg.startsWith("--showFiles=")){
				showFiles = Boolean.parseBoolean(arg.split("=")[1]);
			}else if (arg.startsWith("--verbose=")){
				verbose = Boolean.parseBoolean(arg.split("=")[1]);
			}else if (arg.startsWith("--doAs=")){
				doAs = arg.split("=")[1]; 
			}else if (arg.startsWith("--exclude=")){
				excludeList = Arrays.asList(arg.split("=")[1].split(","));
			}else{
				System.err.println("Argumment not in list of valid arguments, it will be ignored:" + arg);
			}
		}

		if (doAs != null){
			System.setProperty("HADOOP_USER_NAME", doAs);
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

		countEntries = 0;
		System.out.println(directoryInfoToJson(DirectoryContentsUtils.listContents(hdfs,hdfsPath,0,maxLevelThreshold,minSizeThreshold,showFiles,verbose,excludeList)));

		if (verbose){
			Date dateEnd = new Date();
			System.err.println();
			System.err.println("end at:" + dateFormat.format(dateEnd));
			System.err.println("total entries : " + countEntries);
			System.err.println("elapsed time: " + (dateEnd.getTime() - dateStart.getTime()) / 1000.0 + " seconds") ;
		}
	}

	public static String directoryInfoToJson(PathInfo directoryInfo) {
		Gson gson = new GsonBuilder().create();
		return gson.toJson(directoryInfo);
	}

	public static PathInfo listContents(FileSystem hdfs, Path path) throws Exception{
		return listContents(hdfs,path,0,-1,-1,false,false,null);
	}

	public static PathInfo listContents(FileSystem hdfs, Path path, boolean verbose) throws Exception{
		return listContents(hdfs,path,0,-1,-1,false,verbose,null);
	}

	public static boolean isInExclusionList(PathInfo pathInfo, List<String> excludeList){

		boolean found = false;
		for (String excludePath : excludeList){
			if (pathInfo.getFullName().startsWith(excludePath)){
				found = true;
				break;
			}
		}
		return found;

	}

	public static PathInfo listContents(FileSystem hdfs, Path path, int currentLevel, int maxLevelThreshold, long minSizeThreshold, boolean showFiles, boolean verbose, List<String> excludeList) throws Exception{

		countEntries++;

		PathInfo pathInfo = new PathInfo();
		ArrayList<PathInfo> children = new ArrayList<PathInfo>();

		try{
			ContentSummary summary = hdfs.getContentSummary(path);
			FileStatus fileStatus = hdfs.getFileStatus(path);

			long totalLength = summary.getLength();
			long totalSpaceConsumed = summary.getSpaceConsumed();
			pathInfo.setName(path.getName());
			pathInfo.setFullName(fileStatus.getPath().toUri().getPath());
			pathInfo.setDirectory(fileStatus.isDirectory());
			pathInfo.setLength(totalLength);
			pathInfo.setSpaceConsumed(totalSpaceConsumed);
			pathInfo.setNumberOfFiles(summary.getFileCount());
			long dirCount = summary.getDirectoryCount();
			if (dirCount > 0){
				dirCount -= 1;
			}
			pathInfo.setNumberOfSubDirectories(dirCount);

			if (verbose){
				System.err.println("Processing dir: " + pathInfo.getFullName());
			}

			if (pathInfo.isDirectory()){
				if (excludeList != null && isInExclusionList(pathInfo,excludeList)){
					pathInfo.setMessage("Drill down stopped due to file/folder is listed in exclusion list");
				} else if (maxLevelThreshold > -1 && currentLevel >= maxLevelThreshold){
					pathInfo.setMessage("Drill down stopped due to maxLevelThreshold achieved (" + maxLevelThreshold + ")");
					if (verbose){
						System.err.println("Drill down stopped due to maxLevelThreshold achieved (" + maxLevelThreshold + ")");
					}
				}else if (minSizeThreshold > -1 && totalLength < minSizeThreshold){
					pathInfo.setMessage("Drill down stopped due to minSizeThreshold achieved (" + minSizeThreshold + ")");
					if (verbose){
						System.err.println("Drill down stopped due to minSizeThreshold achieved (" + minSizeThreshold + ")");
					}
				}else{
					long subDirsLength=0;
					long subDirsSpaceConsumed=0;
					long files=0;
					for (FileStatus fs : hdfs.listStatus(path)){
						if (fs.isDirectory()){
							PathInfo child = listContents(hdfs,fs.getPath(), currentLevel+1, maxLevelThreshold, minSizeThreshold, showFiles, verbose, excludeList);
							children.add(child);
							subDirsLength += child.getLength();
							subDirsSpaceConsumed += child.getSpaceConsumed();
						}else if (showFiles){
							PathInfo child = listContents(hdfs,fs.getPath(), currentLevel+1, maxLevelThreshold, minSizeThreshold, showFiles, verbose, excludeList);
							children.add(child);
						}else{
							files++;
						}
					}
					if (!showFiles && files > 0){
						if (dirCount == 0){
							pathInfo.setMessage("this directory doesn't have sub-directories");
						}else{
							PathInfo multipleFilesInfo = new PathInfo("+(" + files + " files)", hdfs.getFileStatus(path).getPath().toUri().getPath(),
									false,totalLength-subDirsLength,totalSpaceConsumed-subDirsSpaceConsumed,files, "this entry represents multiple files to reduce visualization pressure");
							children.add(multipleFilesInfo);
						}
					}
				}
				pathInfo.setChildren(children);
			}
		}catch (FileNotFoundException e){
			//IGNORE FILE NOT FOUND, IT HAPPENS WHEN TEMPORARY FILES/FOLDERS WERE DELETED BETWEEEN DIRECTORY LIST AND SUMMARY REQUEST
			pathInfo.setMessage("FileNotFound error, probably a temporary file/folder deleted in the middle of processing");
		}

		return pathInfo;
	}
}