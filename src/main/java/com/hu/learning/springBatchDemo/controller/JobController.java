package com.hu.learning.springBatchDemo.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
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
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;

@RestController
@RequestMapping("/jobs")
@Slf4j
public class JobController {

    @Autowired
    JobLauncher jobLauncher;

    @Autowired
    Job job;

    @PostMapping("/importCustomer")
    public ResponseEntity<String> importCSVtoDB(@RequestParam MultipartFile multipartFile) throws IOException {

        //to append the path from JobParameter
        String inputFile = multipartFile.getOriginalFilename();
        File fileFromInput = new File(System.getProperty("user.dir") + "/" + inputFile);
        multipartFile.transferTo(fileFromInput);

        JobParameters jobParameters = new JobParametersBuilder()
                .addString("fileName", fileFromInput.getAbsolutePath())
                .addLong("startAt", System.currentTimeMillis()).toJobParameters();

        try {
            jobLauncher.run(job, jobParameters);
        } catch (JobExecutionAlreadyRunningException | JobRestartException | JobInstanceAlreadyCompleteException |
                 JobParametersInvalidException e) {
            log.info("Error >>> " + e.getMessage());
        }

        fileFromInput.delete();

        return new ResponseEntity<>("Job Run Successfully", HttpStatus.OK);
    }
}
