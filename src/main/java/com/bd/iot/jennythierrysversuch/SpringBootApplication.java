package com.bd.iot.jennythierrysversuch;

import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.InboundChannelAdapter;
import org.springframework.integration.annotation.Poller;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.RouterSpec;
import org.springframework.integration.dsl.support.Consumer;
import org.springframework.integration.file.FileReadingMessageSource;
import org.springframework.integration.file.filters.SimplePatternFileListFilter;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.integration.router.ExpressionEvaluatingRouter;
import org.springframework.integration.router.HeaderValueRouter;
import org.springframework.integration.splitter.AbstractMessageSplitter;
import org.springframework.integration.transformer.AbstractPayloadTransformer;
import org.springframework.messaging.*;
import org.springframework.messaging.support.GenericMessage;

import java.io.File;
import java.nio.file.Files;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@org.springframework.boot.autoconfigure.SpringBootApplication
public class SpringBootApplication {

    private static final String INBOUND_PATH ="/sys/bus/w1/devices/28-000007409a74/";
    public static final String NAN = "nan";
    public static final String TOPIC_TEMPERATURE = "temperature";
    public static final String TOPIC_ALERT = "alert";


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
                .split(messageSplitter())
                .route("headers['topic']", new Consumer<RouterSpec<ExpressionEvaluatingRouter>>() {
                    @Override
                    public void accept(RouterSpec<ExpressionEvaluatingRouter> expressionEvaluatingRouterRouterSpec) {
                        expressionEvaluatingRouterRouterSpec.channelMapping(TOPIC_TEMPERATURE, TOPIC_TEMPERATURE);
                        expressionEvaluatingRouterRouterSpec.channelMapping(TOPIC_ALERT, TOPIC_ALERT);
                    }
                })
                .get();
    }

    @Bean
    public MessageChannel mqttOutboundChannel() {
        return new DirectChannel();
    }

    @Bean
    MessageHeaders messageHeadersTemperature(){
        Map<String, Object> map = new HashMap<>();
        map.put("topic", TOPIC_TEMPERATURE);
        return new MessageHeaders(map);
    }
    @Bean
    MessageHeaders messageHeadersAlert(){
        Map<String, Object> map = new HashMap<>();
        map.put("topic", TOPIC_ALERT);
        return new MessageHeaders(map);
    }

    @Bean
    public AbstractMessageSplitter messageSplitter(){
        return new AbstractMessageSplitter() {
            @Override
            protected Object splitMessage(Message<?> message) {
               String payload = (String) message.getPayload();
                Collection<Message<String>> results = new ArrayList<>();
                if(NAN.equals(payload)){
                    results.add(new GenericMessage<String>(NAN,messageHeadersTemperature()));
                } else {
                    String temp = payload.split(";")[0];
                    if (30 <= Double.valueOf(temp)){
                        results.add(new GenericMessage<String>("on", messageHeadersAlert()));
                    } else
                    {
                        results.add(new GenericMessage<String>("off",messageHeadersAlert()));
                    }
                    results.add(new GenericMessage<String>((String) message.getPayload(), messageHeadersTemperature()));
                }
                return results;


            }
        };
    }

    @Bean
    @InboundChannelAdapter(value = "fileInputChannel", poller = @Poller(fixedDelay = "10000"))
    public MessageSource<File> fileReadingMessageSource() {
        FileReadingMessageSource source = new FileReadingMessageSource();
        source.setDirectory(new File(INBOUND_PATH));
        source.setFilter(new SimplePatternFileListFilter("w1_slave"));
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
    @ServiceActivator(inputChannel = TOPIC_ALERT)
    public MessageHandler mqttAlertOutbound() {
        MqttPahoMessageHandler messageHandler =
                new MqttPahoMessageHandler("testClient", mqttClientFactory());
        messageHandler.setAsync(true);
        messageHandler.setDefaultTopic("bdch/bern/team2/alert");
        return messageHandler;
    }
    @Bean
    @ServiceActivator(inputChannel = TOPIC_TEMPERATURE)
    public MessageHandler mqttTemperatureOutbound() {
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

                return NAN;
            }

        };
    }

}