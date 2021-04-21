package com.redhat.emergency.response.source.serialization;

import java.io.UnsupportedEncodingException;
import java.util.Map;

import org.apache.commons.text.StringEscapeUtils;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

public class QuotedStringDeserializer implements Deserializer<String> {

    private String encoding = "UTF8";

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
        Object encodingValue = configs.get(propertyName);
        if (encodingValue == null)
            encodingValue = configs.get("deserializer.encoding");
        if (encodingValue instanceof String)
            encoding = (String) encodingValue;
    }

    @Override
    public String deserialize(String topic, byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            else {
                String deserialized = new String(data, encoding);
                if (deserialized.startsWith("\"")) {
                    String unescaped = StringEscapeUtils.unescapeJson(deserialized);
                    return unescaped.substring(1, unescaped.length()-1);
                }
                return deserialized;
            }
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + encoding);
        }
    }

    @Override
    public void close() {
        // nothing to do
    }
}
