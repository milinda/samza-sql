package org.apache.samza.sql;


import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.producer.KeyedMessage;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.samza.SamzaException;
import org.apache.samza.config.MapConfig;
import org.apache.samza.job.ApplicationStatus;
import org.apache.samza.job.StreamJob;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class TestQueryExecutor extends TestQueryExecutorBase {

  @Test
  public void testFilter() throws Exception {
    String ordersTopic = "Orders";
    final String schema = salesSchema(zkServer.connectString(), brokers);
    System.out.println(schema);
    MockSamzaSQLConnection samzaSQLConnection = new MockSamzaSQLConnection(schema);
    final Map<String, String> configurations = new HashMap<>();
    configurations.put("samza.sql.metastore.zk.connect", zkServer.connectString());
    QueryExecutor queryExecutor = new QueryExecutor(samzaSQLConnection, new MapConfig(configurations));
    final StreamJob job = queryExecutor.executeQuery("insert into filtered select * from orders where units > 5");
    ApplicationStatus status = job.waitForStatus(ApplicationStatus.Running, 20 * 60000);
    if (status != null && status == ApplicationStatus.Running) {
      // Send some messages
      List<Map<Object, Object>> inputMessages = readMessages("/filter-test.json", "input");
      List<KeyedMessage> orderMessages = new ArrayList<KeyedMessage>();
      for (Map<Object, Object> msg : inputMessages) {
        orderMessages.add(createOrderFrom(ordersTopic, msg));
      }

      publish(ordersTopic, orderMessages);

      // Wait for output to appear
      waitForTopic("filtered");

      // Verify
      verify("filtered", "filtergroup0", new QueryOutputVerifier() {
        @Override
        public void verify(KafkaStream<byte[], byte[]> stream) throws Exception {
          List<Map<Object, Object>> output = readMessages("/filter-test.json", "output");

          ConsumerIterator<byte[], byte[]> consumerIterator = stream.iterator();

          if(consumerIterator.hasNext()) {
            GenericRecord filteredOrder = filteredOrderFrom(consumerIterator.next().message());
            Assert.assertTrue("Filter query was successful.", filteredOrder.get("orderId").equals(Integer.valueOf((String)output.get(0).get("orderId"))));
          }
        }
      });
      // TODO: How to cleanup topics
    } else {
      Assert.fail("Streaming job is not running after 20 seconds.");
    }

    job.kill();
  }

  protected GenericRecord filteredOrderFrom(byte[] message) throws IOException {
    Schema ordersSchema = new Schema.Parser().parse(resourceToString("/filtered.avsc"));
    GenericDatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(ordersSchema);
    return reader.read(null, DecoderFactory.get().binaryDecoder(message, null));
  }

  protected KeyedMessage createOrderFrom(String topic, Map<Object, Object> order) throws IOException {
    Schema ordersSchema = new Schema.Parser().parse(resourceToString("/orders.avsc"));
    GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>(ordersSchema);

    GenericRecordBuilder recordBuilder = new GenericRecordBuilder(ordersSchema);
    recordBuilder.set("orderId", Integer.valueOf((String) order.get("orderId")));
    recordBuilder.set("productId", Integer.valueOf((String) order.get("productId")));
    recordBuilder.set("rowtime", Long.valueOf((String) order.get("rowtime")));
    recordBuilder.set("units", Integer.valueOf((String) order.get("units")));

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);

    try {
      writer.write(recordBuilder.build(), encoder);
      encoder.flush();
      return new KeyedMessage(topic, ((String) order.get("productId")).getBytes(), out.toByteArray());
    } catch (IOException e) {
      String errMsg = "Cannot perform Avro binary encode.";
      throw new SamzaException(errMsg, e);
    }
  }


  @Test
  public void testKafka() throws Exception {
    String topic = "test";
    String consumerGroup = "group0";
    // Create a topic
    createTopic(topic, 1);

    // send a message
    KeyedMessage<Integer, byte[]> data = new KeyedMessage(topic, "test-message".getBytes(StandardCharsets.UTF_8));

    List<KeyedMessage> messages = new ArrayList<KeyedMessage>();
    messages.add(data);

    publish(topic, messages);

    deleteConsumerGroup(consumerGroup);

    verify(topic, consumerGroup, new QueryOutputVerifier() {
      @Override
      public void verify(KafkaStream<byte[], byte[]> stream) {
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

        if (iterator.hasNext()) {
          String msg = new String(iterator.next().message(), StandardCharsets.UTF_8);
          Assert.assertEquals("test-message", msg);
        } else {
          Assert.fail("No messages found");
        }
      }
    });
  }
}
