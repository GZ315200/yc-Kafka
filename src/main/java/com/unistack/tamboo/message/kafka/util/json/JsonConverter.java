package com.unistack.tamboo.message.kafka.util.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.unistack.tamboo.commons.utils.errors.DataException;
import com.unistack.tamboo.commons.utils.errors.SerializationException;
import com.unistack.tamboo.commons.utils.json.serialization.JsonDeserializer;
import com.unistack.tamboo.commons.utils.json.serialization.JsonSerializer;

import java.io.IOException;
import java.util.Map;

/**
 * @author Gyges Zean
 * @date 2018/5/11
 */
public class JsonConverter implements Converter {

    private final JsonSerializer serialize = new JsonSerializer();

    private final JsonDeserializer deserializer = new JsonDeserializer();

    private static ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        //控制用户是否使用json进行数据转换
        //  TODO
    }

    @Override
    public byte[] fromConnectData(String key, Object value) {
        if (!(value instanceof byte[])) {
            throw new DataException("Input data must be specified as a byte[] value.");
        }
        JsonNode jsonNode = convertToJson(key, value);
        return serialize.serialize(key, jsonNode);
    }

    @Override
    public GenericRecord toConnectData(String topic, byte[] value) {
        JsonNode jsonValue;
        try {
            jsonValue = deserializer.deserialize(topic, value);
        } catch (SerializationException e) {
            throw new DataException("Converting byte[] to Kafka Runner data failed due to serialization error: ", e);
        }

        return fromJSON(jsonValue);
    }


    private GenericRecord fromJSON(JsonNode jsonValue) {
        GenericRecord record = null;
        try {
            if (jsonValue != null) {
                byte[] contents = jsonValue.get("content").binaryValue();
                String topic = jsonValue.get("topic").asText();
                if (topic != null && contents != null) record = GenericRecord.
                        instance(topic)
                        .content(contents)
                        .build();
            }
        } catch (IOException e) {
            throw new DataException("Converting string to object failed.", e);
        }
        return record;
    }


    public Object fromByte(byte[] arrays,Class type) {
        try {
            return objectMapper.readValue(arrays,type);
        } catch (IOException e) {
            throw new DataException("Converting string to object failed.", e);
        }
    }



    /**
     * convert object value to JsonNode
     *
     * @param key
     * @param logicalValue
     * @return
     */
    private JsonNode convertToJson(String key, Object logicalValue) {
        if (logicalValue == null) {
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }
        GenericRecord dataModel = GenericRecord
                .instance(key)
                .content(logicalValue)
                .build();
        return objectMapper.convertValue(dataModel, JsonNode.class);
    }


    public static void main(String[] args) throws IOException {
        JsonConverter jsonConverter = new JsonConverter();
        String s = "hello,world";
        byte[] value = jsonConverter.fromConnectData("test", s.getBytes());
//fo());

        System.out.println(value);

        GenericRecord genericRecord = jsonConverter.toConnectData("test", value);
//
        System.out.println(genericRecord.getTopic());
        System.out.println(new String(genericRecord.getContent()));
        System.out.println(genericRecord.getSize());
        System.out.println(genericRecord.getTimestamp());

//        System.out.println(new String(record.getContent()));
//        System.out.println(record.getSize());
//        System.out.println(record.getTopic());
//        System.out.println(new String(record.getContent()));

//        System.out.println(objectMapper.writeValueAsString(record));


//       String s =  "{\"topic\":\"test\",\"metaInfo\":null,\"content\":\"dGVzdA==\",\"timestamp\":null}";
//        System.out.println(jsonConverter.fromConnectData("test",s.getBytes()));


    }
}
