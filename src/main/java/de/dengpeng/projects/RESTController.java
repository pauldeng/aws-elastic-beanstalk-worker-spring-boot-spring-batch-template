package de.dengpeng.projects;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;

@RestController
public class RESTController {
	
    @Autowired
    JobLauncher jobLauncher;
    
    @Autowired
    Job job1;

    @Autowired
    Job job2;
    
    @Autowired
    Job job3;
    
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
    
    @RequestMapping("/job3/{input_file_name}")
    @ResponseBody
    String requestJob3(@PathVariable("input_file_name") String inputFileName) throws JobExecutionAlreadyRunningException, JobRestartException, JobInstanceAlreadyCompleteException, JobParametersInvalidException{
    	JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
    	jobParametersBuilder.addString("INPUT_FILE_PATH", inputFileName);
    	jobParametersBuilder.addLong("TIMESTAMP",new Date().getTime());
    	
    	jobLauncher.run(job3, jobParametersBuilder.toJobParameters());
        return "Job3!";
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
    
    @RequestMapping(value="/sqs", method=RequestMethod.POST)
    @ResponseBody
    public ResponseEntity<Void> sqsMessageHandler(
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
        	
        	File localFile = retrieveS3File(sqsdMessageBody);
        	
        	if(localFile != null){
        		System.out.println("File downloaded: " + localFile.getAbsolutePath());
        		
        		// verify it
        		
        		
        		// extract it
        		
        		
        		// process it
        		
        		
        		// do you own things
        		
        	}
        	
            return new ResponseEntity<Void>(HttpStatus.OK);
        }catch(Exception ex){
            String errorMessage;
            errorMessage = ex + " <== error";
    		System.out.println("XXXXXXXXX");
    		System.out.println(errorMessage);
    		System.out.println("XXXXXXXXX");
            
            return new ResponseEntity<Void>(HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

	private File retrieveS3File(String sqsdMessageBody) throws UnsupportedEncodingException {
		File localFile = null;
		
    	if(!sqsdMessageBody.isEmpty()){
    		
    		AmazonS3 s3 = new AmazonS3Client();
    		
    		List<S3EventNotificationRecord> records = S3EventNotification.parseJson(sqsdMessageBody).getRecords();
    		        		
    		S3EventNotificationRecord firstRecord = records.get(0);
    		
    		String bucketName = firstRecord.getS3().getBucket().getName();
    		
    		String objectRegion = firstRecord.getAwsRegion();
    		Region s3Region = Region.getRegion(Regions.fromName(objectRegion));
    		s3.setRegion(s3Region);
    		
    		// Object key may have spaces or unicode non-ASCII characters.
            String keyName = firstRecord.getS3().getObject().getKey().replace('+', ' ');
            keyName = URLDecoder.decode(keyName, "UTF-8");                
            
            localFile = new File(keyName);
            
            System.out.println("Downloading file: " + objectRegion + "/" + bucketName + "/" + keyName);
            s3.getObject(new GetObjectRequest(bucketName, keyName), localFile);
            
            if(!localFile.canRead()){
            	localFile = null;
            }
    	}
		return localFile;
	}
    
}
