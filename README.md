# Aerospike travel aggregation

This is a simple example that demonstrates a "groupby" and "aggregation" of flight statistics.

##Data

The inout data consists of:

| timestamp | origin | destination | origin+destination | # Pax traveling | Association|
|-----------|--------|-------------|--------------------|---------------|------------|
|3501892936,92,12:00.0|LAX|DFW|LAXDFW|1|Google|
|3501892964,92,13:00.0|LAX|DFW|LAXDFW|1|Google|
|3501892970,92,13:00.0|LAX|DFW|LAXDFW|3|Google|
|3501893361,92,34:00.0|LAX|DFW|LAXDFW|2|Google|
|3501896150,92,09:00.0|LAX|DFW|LAXDFW|1|MSN|
|3501896170,92,10:00.0|LAX|DFW|LAXDFW|2|MSN|

##Output
The outout is a nested map structure first grouped by source and destination, then by association, finally aggregated by PAX count
```
{
LAXDFW={
	Google={
		pax={
			1=2, 
			2=1, 
			3=1
		}
	}, 
	MSN={
		pax={
			1=1, 
			2=1}
		}, 
	dest=DFW, 
	orig=LAX
	}
}
```

## Build instructions

Clone the GitHub repository at https://github.com/helipilot50/aerospike-travel-aggregation.git

Execute the command:
 
```bash
mvn clean package
```

A runnable Jar will be produced in the `target` subdirectory, run this with the following command:
```bash
java -jar target/aerospike-travel-aggregation-1.0.0-full.jar
```

The example will run with default options, you can use the following options:
```bash
options:
-h,--host <arg>       Server hostname (default: 127.0.0.1)
-n,--namespace <arg>  Namespace (default: test)
-p,--port <arg>       Server port (default: 3000)
-s,--set <arg>        Set (default: demo)
-u,--usage            Print usage.
```

