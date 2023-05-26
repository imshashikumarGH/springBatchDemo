package com.hu.learning.springBatchDemo.config;

import com.hu.learning.springBatchDemo.entity.Customer;
import com.hu.learning.springBatchDemo.listener.CustomSkipListener;
import com.hu.learning.springBatchDemo.partition.RangePartitioner;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.SkipListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.TaskExecutorPartitionHandler;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.File;
import java.time.format.DateTimeParseException;

@Slf4j
@Configuration
public class BatchConfig {

    @Autowired
    JobRepository jobRepository;

    @Autowired
    PlatformTransactionManager platformTransactionManager;

    @Autowired
    CustomerWriter customerWriter;

    @Bean
    @StepScope
    public FlatFileItemReader<Customer> customerReader(@Value("#{jobParameters['fileName']}") String pathToFile) {
        FlatFileItemReader<Customer> customerFlatFileItemReader = new FlatFileItemReader<>();
        customerFlatFileItemReader.setResource(new FileSystemResource(new File(pathToFile)));
        customerFlatFileItemReader.setName("CSVReader");
        customerFlatFileItemReader.setLinesToSkip(1);
        customerFlatFileItemReader.setLineMapper(lineMapper());
        return customerFlatFileItemReader;
    }

    private LineMapper<Customer> lineMapper() {
        DefaultLineMapper<Customer> customerDefaultLineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();
        delimitedLineTokenizer.setDelimiter(",");
        delimitedLineTokenizer.setStrict(false);
        delimitedLineTokenizer.setNames("id", "firstName", "lastName", "email", "gender", "contactNo", "country", "dob");

        BeanWrapperFieldSetMapper<Customer> customerBeanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
        customerBeanWrapperFieldSetMapper.setTargetType(Customer.class);

        customerDefaultLineMapper.setLineTokenizer(delimitedLineTokenizer);
        customerDefaultLineMapper.setFieldSetMapper(customerBeanWrapperFieldSetMapper);
        return customerDefaultLineMapper;
    }

    @Bean
    public CustomerProcessor customerProcessor() {
        return new CustomerProcessor();
    }

    @Bean
    public RangePartitioner partitioner() {
        return new RangePartitioner();
    }

    @Bean
    public PartitionHandler partitionHandler(FlatFileItemReader<Customer> customerReader) {
        TaskExecutorPartitionHandler taskExecutorPartitionHandler = new TaskExecutorPartitionHandler();
        taskExecutorPartitionHandler.setGridSize(4);
        taskExecutorPartitionHandler.setTaskExecutor(taskExecutor());
        taskExecutorPartitionHandler.setStep(slaveStep(customerReader));
        return taskExecutorPartitionHandler;
    }

    @Bean
    public Step slaveStep(FlatFileItemReader<Customer> customerReader) {
        return new StepBuilder("slaveStep", jobRepository)
                .<Customer, Customer>chunk(250, platformTransactionManager)
                .reader(customerReader)
                .processor(customerProcessor())
                .writer(customerWriter)
                .faultTolerant()
                .listener(skipListener())
                .skipLimit(1000)
                .skip(DateTimeParseException.class)
                //.noSkip(IllegalArgumentException.class)
                .build();
    }

    @Bean
    public Step masterStep(FlatFileItemReader<Customer> customerReader) {
        return new StepBuilder("master-step-to-import-Csv-customer-info", jobRepository)
                .partitioner(slaveStep(customerReader).getName(), partitioner())
                .partitionHandler(partitionHandler(customerReader))
                .build();
    }

    @Bean
    public Job job(FlatFileItemReader<Customer> customerReader) {
        return new JobBuilder("job-to-import-CSV-customer-info", jobRepository)
                .flow(masterStep(customerReader))
                .end()
                .build();
    }

    // this will execute the process in much lesser time due to concurrency and multiple thread in this case- 10 threads
    // data won't be in sequence it just based on thread and CPU allocation
    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setMaxPoolSize(4);
        taskExecutor.setCorePoolSize(4);
        taskExecutor.setQueueCapacity(4);
        return taskExecutor;
    }

    @Bean
    public SkipListener skipListener() {
        return new CustomSkipListener();
    }
}
