# HdfsUtils
Project/scripts to help analyse HDFS data.

- First feature is a D3 SunBrust Visualization showing HDFS space usage and/or number of files
- Snapshot space consumption overhead analyzer (from [this discussion](https://community.hortonworks.com/questions/24063/hdfs-snapshot-space-consumption-report.html) is comming next (stay tunned).

##Options to run
1- Zeppelin notebook
Just import URL below into your zeppelin:<br>
https://raw.githubusercontent.com/gbraccialli/HdfsUtils/master/zeppelin/hdfs-d3.json

2- Build from source, running in command line and using html file
###Building
```sh
git clone https://github.com/gbraccialli/HdfsUtils.git
cd HdfsUtils
mvn clean package
````
###Basic usage
```sh
java -jar target/gbraccialli-hdfs-utils-with-dependencies.jar \
  --path=/ \
  --maxLevelThreshold=-1  \
  --minSizeThreshold=-1  \
  --showFiles=false   \
  --verbose=true > out.json  
```
###Visualizing
Open html/hdfs_sunburst.html in your browser and point to your .json file, or copy/paste json content on right load options

3- Command line options


#Special thanks to:
- [Dave Patton](https://github.com/dp1140a) who first created [HDP-Viz](https://github.com/dp1140a/HDP-Viz) where I got insipered and copied lots of code
- [Ali Bajwa](https://github.com/abajwa-hw) who created [ambari stack for Dave's project](https://github.com/abajwa-hw/hdpviz) (and helped me get it working)
- [David Streever](https://github.com/dstreev) who created (or forked) [hdfs-cli](https://github.com/dstreev/hdfs-cli), where I also copied lots of code
