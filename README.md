# CS4371-homework2-Moktar-Subre
I had removed the -99's from the temperature

------
Start Up Hadoop
-------
ssh localhost
hdfs namenode -format
start-dfs.sh
start-yarn.sh

----
Creating directory and storing files
----
change the directory to where you stored the files
hdfs dfs -mkdir /file
hdfs dfs -put city_temperature.csv /file
hdfs dfs -put country-list.csv /file

---
Running Jar Files
---
Change to where the jar files are stored
hadoop jar assignment2partA.jar Region /input/city_temperature.csv /output_A
hadoop jar assignment2partB.jar yearTemp /input/city_temperature.csv /output_B
hadoop jar assignment2partC.jar City /input/city_temperature.csv /output_C
hadoop jar assignemnt2partD.jar capitalTemp /input/city_temperature.csv /input/country-list.csv /output_D

----
Collecting the files
-----
Move to where you want to store the files
hdfs dfs -get /output_A ~/Documents
hdfs dfs -get /output_B ~/Documents
hdfs dfs -get /output_C ~/Documents
hdfs dfs -get /output_D ~/Documents
