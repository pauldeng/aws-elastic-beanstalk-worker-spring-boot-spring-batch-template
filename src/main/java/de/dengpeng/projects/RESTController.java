package de.dengpeng.projects;

import java.io.File;
import java.net.URLDecoder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

@RestController
public class RESTController {
	
    @Autowired
    JobLauncher jobLauncher;
    
    @Autowired
    Job job1;

    @Autowired
    Job job2;
    
    @RequestMapping("/job1")
    @ResponseBody
    String requestJob1() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException{
        jobLauncher.run(job1, createInitialJobParameterMap());
        return "Job1!";
    }

    @RequestMapping("/job2")
    @ResponseBody
    String requestJob2() throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException{
        jobLauncher.run(job2, createInitialJobParameterMap());
        return "Job2!";
    }
    
    private JobParameters createInitialJobParameterMap() {
        Map<String, JobParameter> m = new HashMap<>();
        m.put("time", new JobParameter(System.currentTimeMillis()));
        JobParameters p = new JobParameters(m);
        return p;
    }
    
    @RequestMapping(value="/")
    @ResponseBody
    public String index() {
        return "Greetings from aws-elastic-beanstalk-worker-spring-boot-spring-batch-template!";
    }
    
    @RequestMapping(value="/sqs", method=RequestMethod.GET)
    @ResponseBody
    public String sqsMessageHandler_get(@RequestParam("json") String jsonMessage) {
        return "GET Message received " + jsonMessage;
    }
    
    @RequestMapping(value="/sqs", method=RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<?> sqsMessageHandler(
    		@RequestHeader(value="User-Agent",required=false) String sqsdMessageUserAgent, 
    		@RequestHeader(value="X-Aws-Sqsd-Msgid",required=false) String sqsdMessageId, 
    		@RequestHeader(value="X-Aws-Sqsd-Queue",required=false) String sqsdMessageQueueName, 
    		@RequestHeader(value="X-Aws-Sqsd-First-Received-At",required=false) String sqsdMessageReceivedTimestamp, 
    		@RequestHeader(value="X-Aws-Sqsd-Receive-Count",required=false) int sqsdMessageCounts, 
    		@RequestHeader(value="Content-Type",required=false) String sqsdMessageContentType, 
    		@RequestHeader(value="X-Aws-Sqsd-Taskname",required=false) String sqsdMessagePeriodicTaskName, 
    		@RequestHeader(value="X-Aws-Sqsd-Attr-(message-attribute-name)",required=false) String sqsdMessageCustomAttribute1, 
    		@RequestHeader(value="X-Aws-Sqsd-Scheduled-At",required=false) String sqsdMessageTaskSchdeuleTime, 
    		@RequestHeader(value="X-Aws-Sqsd-Sender-Id",required=false) String sqsdMessageSenderId,
    		@RequestBody String sqsdMessageBody) {
    	
        try{
        	
        	if(!sqsdMessageBody.isEmpty()){
        		
        		List<S3EventNotificationRecord> records = S3EventNotification.parseJson(sqsdMessageBody).getRecords();
        		        		
        		S3EventNotificationRecord firstRecord = records.get(0);
        		
        		String bucketName = firstRecord.getS3().getBucket().getName();
        		
        		String s3Region = firstRecord.getAwsRegion();
        		
        		// Object key may have spaces or unicode non-ASCII characters.
                String objectName = firstRecord.getS3().getObject().getKey().replace('+', ' ');
                objectName = URLDecoder.decode(objectName, "UTF-8");
                
                System.out.println("Downloading an object from: " + s3Region + "/" + bucketName + "/" + objectName);
                
                
                // add your business logic here
                

        	}
            return new ResponseEntity<>(HttpStatus.OK);
        }catch(Exception ex){
            String errorMessage;
            errorMessage = ex + " <== error";
            return new ResponseEntity<>(errorMessage, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    
}
