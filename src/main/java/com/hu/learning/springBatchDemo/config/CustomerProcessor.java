package com.hu.learning.springBatchDemo.config;

import com.hu.learning.springBatchDemo.entity.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;


@Slf4j
public class CustomerProcessor implements ItemProcessor<Customer, Customer> {
    @Override
    public Customer process(Customer customer) throws Exception {
        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");
        if (LocalDate.parse(customer.getDob(), dateFormatter).isBefore(LocalDate.now().minusYears(18))) {
            log.info("customer info - {} ", customer);
            return customer;
        }
        return null;
    }
}
