package com.github.gbraccialli.hdfs;

import java.util.ArrayList;

import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class DirectoryContentsUtils {

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
			if (maxLevelThreshold > -1 && currentLevel > maxLevelThreshold){
				dir.setMessage("Drill down stopped due to maxLevelThreshold achieved (" + maxLevelThreshold + ")");
			}else if (minSizeThreshold > -1 && totalLength < minSizeThreshold){
				dir.setMessage("Drill down stopped due to minSizeThreshold achieved (" + minSizeThreshold + ")");
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