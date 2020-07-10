package com.example.sparkdemo;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.hadoop.batch.scripting.ScriptTasklet;
import org.springframework.data.hadoop.scripting.HdfsScriptRunner;
import org.springframework.scripting.ScriptSource;
import org.springframework.scripting.support.ResourceScriptSource;
import org.springframework.stereotype.Component;
import sun.font.Script;

import java.util.HashMap;
import java.util.Map;

@Component
public class SparkYarnConfiguration {
    @Autowired
    private org.apache.hadoop.conf.Configuration hadoopConfig;
    String inputDir;
    String inputFileName;
    String inputLocalDir;
    String outputDir;
    String sparkAssembly;

    //Job definition
    @Bean
    Job tweetHashtags(JobBuilderFactory jobs, Step initScript, Step sparkTopHashtags)throws Exception{
        return jobs.get("TweetTopHashtags")
                .start(initScript)
                .next(sparkTopHashtags)
                .build();
    }


    @Bean
    Step initScript(StepBuilderFactory steps, Tasklet scriptTasklet)throws Exception{
        return steps.get("initScript")
                .tasklet(scriptTasklet)
                .build();
    }

    @Bean
    ScriptTasklet scriptTasklet(HdfsScriptRunner scriptRunner){
        ScriptTasklet scriptTasklet = new ScriptTasklet();
        scriptTasklet.setScriptCallback(scriptRunner);
        return scriptTasklet;
    }

    @Bean HdfsScriptRunner scriptRunner(){
        ScriptSource script = new ResourceScriptSource(new ClassPathResource("fileCopy.js"));
        HdfsScriptRunner scriptRunner = new HdfsScriptRunner();
        scriptRunner.setConfiguration(hadoopConfig);
        scriptRunner.setLanguage("javascript");
        Map<String, Object> arguments = new HashMap<>();
        arguments.put("source", inputLocalDir);
        arguments.put("file", inputFileName);
        arguments.put("indir", inputDir);
        arguments.put("outdir", outputDir);
        scriptRunner.setArguments(arguments);
        scriptRunner.setScriptSource(script);
        return scriptRunner;
    }


}
