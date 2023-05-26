package com.hu.learning.springBatchDemo.listener;

import com.hu.learning.springBatchDemo.entity.Customer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;

@Slf4j
public class CustomSkipListener implements SkipListener<Customer, Number> {

    @Override
    public void onSkipInRead(Throwable t) {
        SkipListener.super.onSkipInRead(t);
    }

    @Override
    public void onSkipInWrite(Number item, Throwable t) {
        SkipListener.super.onSkipInWrite(item, t);
    }

    @Override
    public void onSkipInProcess(Customer item, Throwable t) {
        // SkipListener.super.onSkipInProcess(item, t);
        log.info("issue in -{} as {}, Error -{}", item.getId(), t.getMessage(), t.toString());
    }
}
