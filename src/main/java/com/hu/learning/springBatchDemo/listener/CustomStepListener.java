package com.hu.learning.springBatchDemo.listener;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CustomStepListener implements StepExecutionListener {
    @Override
    public void beforeStep(StepExecution stepExecution) {
        log.info(" Before Step - {} " + stepExecution.getStepName());
        log.info(" Job Execution - {} " + stepExecution.getJobExecution().getExecutionContext());
        log.info(" Step Execution - {} " + stepExecution.getExecutionContext());
        stepExecution.getExecutionContext().put("DummyKey", "Adding dummy value in stepContext");
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        log.info(" After Step - {} ", stepExecution.getStepName());
        log.info(" Job Execution - {} ", stepExecution.getJobExecution().getExecutionContext());
        log.info(" Step Execution - {} ", stepExecution.getExecutionContext());
        return StepExecutionListener.super.afterStep(stepExecution);
    }
}

