package org.example;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class Banktransactionproducertest {

    @Test

    public void newRandomTransactionstest {
        ProducerRecord<String, String> record = Banktransactionproducer.newRandomTransaction("ranjith");
        String key = record.key();
        String value = record.value();

        assertEquals(key, "ranjith");

        ObjectMapper mapper = new ObjectMapper();
        try {
            JsonNode node = mapper.readTree(value);
            assertEquals("ranjith",node.get("name").asText());
            assertTrue("Amount shoulb be less than 100", node.get("amount").asInt() < 100);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(value);
    }
}
