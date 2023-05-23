package com.hu.learning.springBatchDemo.repository;

import com.hu.learning.springBatchDemo.entity.Customer;
import org.springframework.data.jpa.repository.JpaRepository;

public interface CustomerRepository extends JpaRepository<Customer, Integer> {
}
