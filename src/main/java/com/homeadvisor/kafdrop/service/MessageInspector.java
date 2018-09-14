/*
 * Copyright 2017 HomeAdvisor, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.homeadvisor.kafdrop.service;

import com.homeadvisor.kafdrop.model.MessageVO;
import com.homeadvisor.kafdrop.model.TopicPartitionVO;
import com.homeadvisor.kafdrop.model.TopicVO;
import com.homeadvisor.kafdrop.util.BrokerChannel;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Service
public class MessageInspector
{
   private final Logger LOG = LoggerFactory.getLogger(getClass());

   @Autowired
   private KafkaMonitor kafkaMonitor;

   @Value("${schema.registry}")
   private String schemaRegistry;


   private SchemaRegistryClient schemaRegistryClient;

   @PostConstruct
   public void init() {
       List<String> urls = Arrays.stream(schemaRegistry.split(",")).map(String::trim).collect(Collectors.toList());
       this.schemaRegistryClient = new CachedSchemaRegistryClient(urls,1000);
   }

   public List<MessageVO> getMessages(String topicName, int partitionId, long offset, long count)
   {
      final TopicVO topic = kafkaMonitor.getTopic(topicName).orElseThrow(TopicNotFoundException::new);
      final TopicPartitionVO partition = topic.getPartition(partitionId).orElseThrow(PartitionNotFoundException::new);

       List<MessageVO> messages = new ArrayList<>();
       try {
           if (!schemaRegistryClient.getAllSubjects().contains(getSchemaName(topicName))) {
               LOG.info("Not an AVRO message, no magic byte found. display the message as it is");
                kafkaMonitor.getBroker(partition.getLeader().getId())
                       .map(broker -> {
                           /** non avro messages*/

                           long currentOffset1 = offset;
                           SimpleConsumer simpleConsumer = new SimpleConsumer(broker.getHost(), broker.getPort(), 10000, 100000, "");

                           final FetchRequestBuilder fetchRequestBuilder = new FetchRequestBuilder()
                                   .clientId("KafDrop")
                                   .maxWait(5000) // todo: make configurable
                                   .minBytes(1);

                           long currentOffset = offset;
                           while (messages.size() < count) {
                               final FetchRequest fetchRequest =
                                       fetchRequestBuilder
                                               .addFetch(topicName, partitionId, currentOffset, 1024 * 1024)
                                               .build();

                               FetchResponse fetchResponse = simpleConsumer.fetch(fetchRequest);

                               final ByteBufferMessageSet messageSet = fetchResponse.messageSet(topicName, partitionId);
                               if (messageSet.validBytes() <= 0) break;

                               int oldSize = messages.size();
                               StreamSupport.stream(messageSet.spliterator(), false)
                                       .limit(count - messages.size())
                                       .map(MessageAndOffset::message)
                                       .map(this::createMessage)
                                       .forEach(messages::add);
                               currentOffset += messages.size() - oldSize;
                           }
                           return messages;
                       })
                       .orElseGet(Collections::emptyList);

           } else {
                LOG.info("Avro schema found");
                kafkaMonitor.getBroker(partition.getLeader().getId())
                       .map(broker -> {
                           Properties consumerProps = new Properties();
                           String brokerAddress = broker.getHost() + ":" + broker.getPort();
                           consumerProps.put("bootstrap.servers", brokerAddress);
                           consumerProps.put("schema.registry.url", schemaRegistry);
                           consumerProps.put("group.id", "KafDrop");
                           consumerProps.put("key.deserializer",
                                   "org.apache.kafka.common.serialization.StringDeserializer");
                           consumerProps.put("value.deserializer",
                                   "io.confluent.kafka.serializers.KafkaAvroDeserializer");
                           consumerProps.put("auto.offset.reset","earliest");
                           KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(consumerProps);
                           TopicPartition partition0 = new TopicPartition(topicName, partitionId);

                           consumer.assign(Arrays.asList(partition0));
                           consumer.poll(1); // a quick poll is needed before we seek tothe position requested
                           consumer.seek(partition0, offset);

                           long currentOffset1 = offset;
                               while (messages.size() < count) {
                                   ConsumerRecords<String, GenericRecord> records = consumer.poll(5000); // todo: make timeout configurable
                                   for (ConsumerRecord<String, GenericRecord> record : records) {
                                       MessageVO mess = new MessageVO();
                                       mess.setMessage(record.value().toString());
                                       mess.setKey(record.key());
                                       mess.setChecksum(0);
                                       mess.setCompressionCodec("none");
                                       messages.add(mess);
                                       if (messages.size() == count)
                                           break;
                                   }
                               }
                           return messages;
                       })
                       .orElseGet(Collections::emptyList);
             }
           }
           catch(Exception e) {
                LOG.error(e.getMessage());
           }
       return messages;
   }

   private MessageVO createMessage(Message message)
   {
      MessageVO vo = new MessageVO();
      if (message.hasKey())
      {
         vo.setKey(readString(message.key()));
      }
      if (!message.isNull())
      {
         vo.setMessage(readString(message.payload()));
      }

      vo.setValid(message.isValid());
      vo.setCompressionCodec(message.compressionCodec().name());
      vo.setChecksum(message.checksum());
      vo.setComputedChecksum(message.computeChecksum());

      return vo;
   }

   private String readString(ByteBuffer buffer)
   {
      try
      {
         return new String(readBytes(buffer), "UTF-8");
      }
      catch (UnsupportedEncodingException e)
      {
         return "<unsupported encoding>";
      }
   }

    public String getSchemaName(String topicName) {

        return String.format("%s-value", topicName);

    }

   private byte[] readBytes(ByteBuffer buffer)
   {
      return readBytes(buffer, 0, buffer.limit());
   }

   private byte[] readBytes(ByteBuffer buffer, int offset, int size)
   {
      byte[] dest = new byte[size];
      if (buffer.hasArray())
      {
         System.arraycopy(buffer.array(), buffer.arrayOffset() + offset, dest, 0, size);
      }
      else
      {
         buffer.mark();
         buffer.get(dest);
         buffer.reset();
      }
      return dest;
   }

}
