What?
====================
This is a custom appender for log4j2. When used with Java/Mule apps, This appender pushes all the application logs to specified CloudWatch.
This code is a fix and upgrade of the code found here : [https://www.javacodegeeks.com/2017/10/integrate-cloudwatch-logs-cloudhub-mule.html](https://www.javacodegeeks.com/2017/10/integrate-cloudwatch-logs-cloudhub-mule.html)
However, there was a small problem when moving the LogEvents from one que to CloudWatch. This is solved by making the LogEvent immutable:

```
loggingEventsQueue.offer(event.toImmutable());
```

Why?
====================

* When using MuleSoft CloudHub, you may want to store the logs somewhere other than in CloudHub.
* Send all log events to a specified CloudWatch instance 
* To make a backup copy for application logs.

How?
==========================
* Build this application using the following command.

```mvn clean install```

Use this dependency in your Java/Mule Applications

```
<dependency>
	<groupId>com.java.javacodegeeks.log4j2.cloudwatch</groupId>
	<artifactId>log4j2-cloudwatch-appender-wf</artifactId>
	<version>1.1.1</version>
	<type>jar</type>
</dependency>
```

* Modify Application's log4j2.xml to add the below appender custom appender config.

```
<Appenders>
	<CLOUDW name="CloudW" logGroupName="<your CloudWatch group name>"
		logStreamName="<your Mule app name>-${sys:environment}"
		awsAccessKey="<your AWS access key>" 
		awsSecretKey="<your AWS secret key>"
		awsRegion="<your AWS region>" 
		endpoint="<your CloudWatch VPC Endpoint>" 
		messagesBatchSize="128"
		queueLength="10000"
		sleepTime="20">
		<PatternLayout
			pattern="%-5p %d [%t] %X{correlationId}%c: %m%n" /> 
	</CLOUDW>
</Appenders>
```
Add this java package in your top level log4j2 configuration element

```
<Configuration packages="com.java.javacodegeeks.log4j2.cloudwatch">
```

Add this custom appender to your Root logger in log4j2.xml.

```
<Root level="INFO">
    <AppenderRef ref="CloudW"/>
</Root>  
     
        (or)

<AsyncRoot level="INFO">
    <AppenderRef ref="CloudW" />
</AsyncRoot>
```

* That's it!

When you run the project with this appender added with your AWS credentials, you should see your app log events flowing into the configured CloudWatch group/logStreamName.