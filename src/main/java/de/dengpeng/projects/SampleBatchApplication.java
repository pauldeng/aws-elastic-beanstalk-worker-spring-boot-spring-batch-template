package de.dengpeng.projects;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.SimpleJob;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableBatchProcessing
public class SampleBatchApplication {

    @Autowired
    private JobBuilderFactory jobs;

    @Autowired
    private StepBuilderFactory steps;

    @Bean(name="job1")
    public Job job1() {
         return jobs
                .get("myJob")
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .next(step2(" hogehoge"))
                .next(steps.get("step3").tasklet((stepContribution, chunkContext) -> {
                    System.out.println("step 3");
                    return RepeatStatus.FINISHED;}).build())
                .build();
    }
    
    @Bean(name="job2")
    public Job job2() {
        SimpleJob job = new SimpleJob();
        job.setRestartable(false);
        
        return jobs
                .get("myJob2")
                .incrementer(new RunIdIncrementer())
                .start(steps.get("step3").tasklet((stepContribution, chunkContext) -> {
                    System.out.println("job2 step");
                    return RepeatStatus.FINISHED;}).build())
                .build();
    }
    
    @Bean(name="job3")
    public Job job3() {
        return jobs
                .get("myJob")
                .incrementer(new RunIdIncrementer())
                .start(step1())
                .next(step2(" job3 sample"))
                .next(steps.get("step3").tasklet((stepContribution, chunkContext) -> {
                	System.out.println(chunkContext.getStepContext().getJobParameters().get("INPUT_FILE_PATH"));
                	System.out.println(chunkContext.getStepContext().getJobParameters().get("TIMESTAMP"));
                    System.out.println("step 3");
                    return RepeatStatus.FINISHED;}).build())
                .build();
    }

    public Step step1() {
        return steps.get("step1").tasklet((stepContribution, chunkContext) -> {
            System.out.println("step 1");
            return RepeatStatus.FINISHED;
        }).build();
    }

    public Step step2(String arg) {
        return steps.get("step2").tasklet((stepContribution, chunkContext) -> {
            System.out.println("step 2" + arg);
            return RepeatStatus.FINISHED;
        }).build();
    }
}
