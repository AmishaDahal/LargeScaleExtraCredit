# Project Description
This project is about UserdefinedAggreagte Function in scala that can be register in python and can be tested in python script with Spark.

# How To compile this Project 

- In order to compile this project, first you need to create a directory like "extracredit",inside that directory create folder like **src/main/scala** inside it  place my scalacode "frequencymode.scala". 

- Then in extracredit folder make a file named "build.sbt" with following dependency :

```
 name := "Mode"
version := "0.1"
scalaVersion := "2.12.20"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "3.5.3" % Provided
)
```
After this in extracredit folder you have to place my finalmode.py file .

# To run this file

Create a jar using : 
```
- sbt clean compile
- sbt package


The jar file will be updated inside the folder target - inside scala-2.12 like "mode_2.12-0.1.jar"

Then add the file location from S3  in start emr and run . In start emr scripts add in step "--jars".
```

# Video Demonstration:
I have uploaded the video named "amishalargescale.mp4" download it locally and play the video. 

# Reference:

- For the regsitration of udaf I took a lot of refrence of blog post some of them :

https://stackoverflow.com/questions/33233737/spark-how-to-map-python-with-scala-or-java-user-defined-functions/33257733
https://stackoverflow.com/questions/33233737/spark-how-to-map-python-with-scala-or-java-user-defined-functions/33257733






  
