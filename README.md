## CS6240 - Fall 2017
### Assignment A6 
### Shreysa Sharma and Jashangeet Singh

## Running 
- Update `SCALA_HOME` and `SPARK_HOME` in the `Makefile`
- Verify `SPARK_CLASS_PATH` (For some installations the lib path is `libexec/jars/*`)
- `make` or `make clean && make build && make run && make report`

## Requirements
- Scala - used version `2.11.11`. *NOTE* using `2.12.4` results in error (`java.lang.NoClassDefFoundError: scala/runtime/LambdaDeserialize`)
- Apache Spark - used version `spark-2.2.0-bin-hadoop2.7`
