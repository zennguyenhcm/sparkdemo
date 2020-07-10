package com.example.sparkdemo;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.context.annotation.Bean;

public class SparkYarnTasklet {
    @Bean
    Step sparkTopHashtags(StepBuilderFactory steps, Tasklet sparkTopHashtagsTasklet)throws Exception  {
        return steps.get("sparkTopHashtags")
                .tasklet(sparkTopHashtagsTasklet)
                .build();
    }


}
