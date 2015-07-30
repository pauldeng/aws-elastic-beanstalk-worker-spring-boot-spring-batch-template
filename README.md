# AWS Elastic Beanstalk Worker Spring Boot with Spring Batch Template
This is a file batch processing application template hosted on AWS Elastic Beanstalk.


## How to Run
1. Edit the project POM file to generate WAR file
2. Log into AWS and create new application
  2.1 Select Worker Environment
  2.2 Create a new IAM role and add your S3 bucket to the IAM policy
  2.3 Choose Tomcat environment and deploy the WAR file
  2.4 Set local disk to your deisred size, 8GB is the minimum
  2.5 SQS message should submit to /sqs endpoint
3. Edit the permission of SQS queue to allow S3 event
4. Edit the properties of your S3 bucket to generate ObjectCreadted(All) event to SQS queue which is created by the Elastic Beanstalk (Do not select the queue with "Dead" in name)
5. Upload a file to your S3 bucket
6. check the log and you should see print out

## Reference
* [http://kagamihoge.hatenablog.com/entry/2015/02/14/144238](http://kagamihoge.hatenablog.com/entry/2015/02/14/144238)
* [springbatchstudy](https://github.com/kagamihoge/springbatchstudy)
* [run spring batch job from the controller](http://stackoverflow.com/questions/28566341/run-spring-batch-job-from-the-controller)


## Have Fun :)