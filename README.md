1.   sessionizer_script.scala in the root directory is the scirpt version of solution for this sessionization change of Weblog.

  1.1  tested run environment:

       a. JDK 1.8.0_74

       b. Spark 1.6.0

       c. windows 7

       d. CPU: i7-4600 2 cores , 8.00GB RAM,  64bit

  1.2  run instructions:

       a.  start spark using [Spark-Home]/bin/spark-shell command

       b.  on the Spark command line type in:
           scala > implicit val tuple: Tuple2[String, String] = ("full path for the large log file", "full path for the output file for collecting results in the Goal part")

       c.  scala > :load [full path of sessionizer_script.scala]
           it will complain if it can not find the implicitly defined Tuple2 as in b.

       d.  the result will be print out at the end of the analyse process which are corresponding
           to the 4 items in the "Processing & Analytical goals" section on the url:
           https://github.com/PaytmLabs/WeblogChallenge
           
       e.  right now the script did not use the output file for persistence of the result instead just print it on the console screen
       f.  the unit test could be found under src/test/scala for the main functionality of this app and can be run conventionally by start SBT console


2.   the weblog folder in the root directory is the standalone APP version of a SBT project for the same purpose as 1. I do not have hadoop on my machine so I did not try the SBT version out on the machine. It should work since the script version works. And all the unit tests are green



