java -jar target/gbraccialli-hdfs-utils-with-dependencies.jar \
  --path=/ \
  --maxLevelThreshold=-1  \
  --minSizeThreshold=-1  \
  --showFiles=false   \
  --verbose=true > out.json  


java -jar target/gbraccialli-hdfs-utils-with-dependencies.jar \
  --path=/ \
  --maxLevelThreshold=5  \
  --minSizeThreshold=50000  \
  --showFiles=false   \
  --verbose=true > out.json  

java -jar target/gbraccialli-hdfs-utils-with-dependencies.jar --confDir=/Users/gbraccialli/Documents/workspace/hdfs-cli/conf --verbose=true

java -jar target/gbraccialli-hdfs-utils-with-dependencies.jar --confDir=/Users/gbraccialli/Documents/workspace/hdfs-cli/conf --verbose=true
