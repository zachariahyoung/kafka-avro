package com.jbhunt.processor;

import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.Processor;
import org.apache.camel.component.kafka.KafkaConstants;
import org.apache.camel.component.kafka.KafkaManualCommit;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaOffsetManagerProcessor implements Processor {
    @Override
    public void process(Exchange exchange) throws Exception {
        Boolean lastOne = exchange.getIn().getHeader(KafkaConstants.LAST_RECORD_BEFORE_COMMIT, Boolean.class);

        if (lastOne != null && lastOne) {
            KafkaManualCommit manual =
                    exchange.getIn().getHeader(KafkaConstants.MANUAL_COMMIT, KafkaManualCommit.class);
            if (manual != null) {
                log.info("manually committing the offset for batch");
                manual.commitSync();
            }
        } else {
            log.info("NOT time to commit the offset yet");
        }
    }
}
