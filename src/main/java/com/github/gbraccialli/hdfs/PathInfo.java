package com.github.gbraccialli.hdfs;

import java.util.ArrayList;

public class PathInfo {

	private String name;
	private String fullName;
	private long length;
	private long spaceConsumed;
	private long numberOfFiles;
	private long numberOfSubDirectories;
	private boolean directory;
	private String message;
	private ArrayList<PathInfo> children;



	public PathInfo(){

	};




	public PathInfo(String name, String fullName, boolean directory, long length, long spaceConsumed,
			long numberOfFiles, String message) {
		this.name = name;
		this.fullName = fullName;
		this.directory = directory;
		this.length = length;
		this.spaceConsumed = spaceConsumed;
		this.numberOfFiles = numberOfFiles;
		this.message = message;

	}




	public String getName() {
		return name;
	}




	public void setName(String name) {
		this.name = name;
	}




	public String getFullName() {
		return fullName;
	}




	public void setFullName(String fullName) {
		this.fullName = fullName;
	}




	public long getLength() {
		return length;
	}




	public void setLength(long length) {
		this.length = length;
	}




	public long getSpaceConsumed() {
		return spaceConsumed;
	}




	public void setSpaceConsumed(long spaceConsumed) {
		this.spaceConsumed = spaceConsumed;
	}




	public long getNumberOfFiles() {
		return numberOfFiles;
	}




	public void setNumberOfFiles(long numberOfFiles) {
		this.numberOfFiles = numberOfFiles;
	}




	public long getNumberOfSubDirectories() {
		return numberOfSubDirectories;
	}




	public void setNumberOfSubDirectories(long numberOfSubDirectories) {
		this.numberOfSubDirectories = numberOfSubDirectories;
	}




	public boolean isDirectory() {
		return directory;
	}




	public void setDirectory(boolean directory) {
		this.directory = directory;
	}

	public String getMessage() {
		return message;
	}


	public void setMessage(String message) {
		this.message = message;
	}


	public ArrayList<PathInfo> getChildren() {
		return children;
	}




	public void setChildren(ArrayList<PathInfo> children) {
		this.children = children;
	}





	public double getAverageReplicaCount(){
		return this.getSpaceConsumed() / (double)this.getSpaceConsumed();
	}

	public double getAverageFileSize(){
		return this.getLength() / this.getNumberOfFiles();
	}


}
