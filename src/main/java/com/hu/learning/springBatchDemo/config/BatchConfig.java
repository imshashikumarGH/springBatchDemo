package com.hu.learning.springBatchDemo.config;

import com.hu.learning.springBatchDemo.entity.Customer;
import com.hu.learning.springBatchDemo.repository.CustomerRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.data.RepositoryItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.transaction.PlatformTransactionManager;

@Slf4j
@Configuration
public class BatchConfig {

    @Autowired
    CustomerRepository customerRepository;

    @Autowired
    JobRepository jobRepository;

    @Autowired
    PlatformTransactionManager platformTransactionManager;

    @Bean
    public FlatFileItemReader<Customer> customerReader() {
        FlatFileItemReader<Customer> customerFlatFileItemReader = new FlatFileItemReader<>();
        customerFlatFileItemReader.setResource(new FileSystemResource("src/main/resources/customer.csv"));
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

    //Multiple item writer class are present;
    @Bean
    public RepositoryItemWriter<Customer> customerWriter() {
        RepositoryItemWriter<Customer> customerRepositoryItemWriter = new RepositoryItemWriter<>();
        customerRepositoryItemWriter.setRepository(customerRepository);
        customerRepositoryItemWriter.setMethodName("save");
        return customerRepositoryItemWriter;
    }

    @Bean
    public Step step() {
        return new StepBuilder("step-to-import-Csv-customer-info", jobRepository)
                .<Customer, Customer>chunk(10, platformTransactionManager)
                .reader(customerReader())
                .processor(customerProcessor())
                .writer(customerWriter())
                .build();
    }

    @Bean
    public Job job() {
        return new JobBuilder("job-to-import-CSV-customer-info", jobRepository)
                .flow(step())
                .end()
                .build();
    }
}
