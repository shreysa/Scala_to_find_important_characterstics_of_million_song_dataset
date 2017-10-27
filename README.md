## CS6240 - Fall 2017
### Assignment A6 
### Shreysa Sharma and Jashangeet Singh

## Running 
-Clone the repository
- Update `SCALA_HOME` and `SPARK_HOME` in the `Makefile`
- Verify `SPARK_CLASS_PATH` (For some installations the lib path is `libexec/jars/*`)
- `make` or `make all` or `make clean && make build && make run && make report`

## Requirements
- Scala - used version `2.11.11`. *NOTE* using `2.12.4` results in error (`java.lang.NoClassDefFoundError: scala/runtime/LambdaDeserialize`)
- Apache Spark - used version `spark-2.2.0-bin-hadoop2.7`
-driver memory and executor memory have been set to 2 GB on the local system on which the program was run, one can run with any configuration but this could affect the time taken to get the desired output
