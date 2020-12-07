package com.jbhunt.route.builder;

import com.jbhunt.AddressDetail;
import com.jbhunt.processor.KafkaOffsetManagerProcessor;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.camel.Exchange;
import org.apache.camel.builder.endpoint.EndpointRouteBuilder;
import org.apache.camel.dataformat.avro.AvroDataFormat;
import org.springframework.stereotype.Component;

@Component
@Slf4j
@RequiredArgsConstructor
public class AddressRouteBulder extends EndpointRouteBuilder {

    private final KafkaOffsetManagerProcessor kafkaOffsetManagerProcessor;

    @Override
    public void configure() throws Exception {

        AvroDataFormat format = new AvroDataFormat(AddressDetail.SCHEMA$);

        from(kafka("ADDRESSDETAILSSTREAM")
                .brokers("local:9092")
                .saslJaasConfig("org.apache.kafka.common.security.plain.PlainLoginModule required username='username' password='password';")
                .securityProtocol("SASL_SSL")
                .saslMechanism("PLAIN")
                .additionalProperties("basic.auth.credentials.source", "USER_INFO")
                .additionalProperties("schema.registry.basic.auth.user.info", "user:password")
                .additionalProperties("isolation.level", "read_committed")
                .schemaRegistryURL("https://confluent.cloud")
                .valueDeserializer(KafkaAvroDeserializer.class.getName())
                .consumersCount(1)
                .autoCommitEnable(false)
                .allowManualCommit(true)
                .seekTo("")
                .autoOffsetReset("earliest")
                .breakOnFirstError(true)
                .groupId("gps"))
                .marshal().avro(format)
                .process(exchange -> {
                    log.info(this.dumpKafkaDetails(exchange));
                })
                .process(kafkaOffsetManagerProcessor)
                .log("Stored sentence with key ${body}");
    }

    private String dumpKafkaDetails(Exchange exchange) {
        AddressDetail addressDetail = exchange.getIn().getBody(AddressDetail.class);

        StringBuilder sb = new StringBuilder();
        sb.append("address:").append(addressDetail.getADDRESSLINE1());
        sb.append("\r\n");
        return sb.toString();
    }
}
