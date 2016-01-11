package com.bd.iot.jennythierrysversuch;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.format.datetime.joda.LocalDateTimeParser;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.annotation.Transformer;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageProducer;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.integration.transformer.AbstractPayloadTransformer;
import org.springframework.integration.transformer.ObjectToStringTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@org.springframework.boot.autoconfigure.SpringBootApplication
public class SpringBootApplication {

    private static final String INBOUND_PATH ="/sys/bus/w1/devices/28-000007409a74/";


    Pattern regex = Pattern.compile("t=(\\d{5})", Pattern.MULTILINE);

    public static void main(String[] args) {
        new SpringApplicationBuilder(SpringBootApplication.class)
                .web(false)
                .run(args);
    }



    @Bean
    public IntegrationFlow myFlow() {
        return IntegrationFlows.from("fileInputChannel")
                .transform(transformer())
                .channel(mqttOutboundChannel())
                .handle(mqttOutbound())
                .get();
    }

    @Bean
    public MessageChannel mqttOutboundChannel() {
        return new DirectChannel();
    }

    @Bean
    @InboundChannelAdapter(value = "fileInputChannel", poller = @Poller(fixedDelay = "1000"))
    public MessageSource<File> fileReadingMessageSource() {
        FileReadingMessageSource source = new FileReadingMessageSource();
        source.setDirectory(new File(INBOUND_PATH));
        source.setFilter(new SimplePatternFileListFilter("*"));
        return source;
    }
    @Bean
    public MessageChannel fileInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        factory.setServerURIs(new String[]{"tcp://broker.hivemq.com:1883"});
        return factory;
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttOutboundChannel")
    public MessageHandler mqttOutbound() {
        MqttPahoMessageHandler messageHandler =
                new MqttPahoMessageHandler("testClient", mqttClientFactory());
        messageHandler.setAsync(true);
        messageHandler.setDefaultTopic("bdch/bern/team2/temperature");
        return messageHandler;
    }

    @Bean
    AbstractPayloadTransformer transformer(){
        return new AbstractPayloadTransformer<File, String >(){
            protected String transformPayload(File var1) throws Exception{
                String temp =  new String(Files.readAllBytes(var1.toPath()));
                System.out.println(temp);
                Matcher matcher = regex.matcher(temp);
                if(matcher.find()){
                    String temperature  = matcher.group(1);
                    return temperature.substring(0,2) + "."+temperature.substring(2,5)+";"+ LocalDateTime.now();
                }

                return "nan";
            }

        };
    }

}