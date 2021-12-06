//package org.learn.flink.feature.serde;
//
//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericDatumWriter;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.avro.io.BinaryEncoder;
//import org.apache.avro.io.EncoderFactory;
//import org.apache.flink.api.common.serialization.DeserializationSchema;
//import org.apache.flink.formats.avro.AvroDeserializationSchema;
//import org.learn.flink.ClickEvent;
//
//import java.io.ByteArrayOutputStream;
//import java.io.IOException;
//
//import static org.junit.Assert.assertEquals;
//import static org.junit.Assert.assertNull;
//
//public class AvroSerdeExample {
//    public static void main(String[] args) throws Exception {
//        DeserializationSchema<ClickEvent> deserializer =
//                AvroDeserializationSchema.forSpecific(ClickEvent.class);
//
//        ClickEvent ck=new ClickEvent();
//        byte[] encodedAddress = writeRecord(ck,ck.getClassSchema());
//        ClickEvent deserializedAddress = deserializer.deserialize(encodedAddress);
//        assertEquals(ck, deserializedAddress);
//    }
//
//    public static byte[] writeRecord(GenericRecord record, Schema schema) throws IOException {
//        ByteArrayOutputStream stream = new ByteArrayOutputStream();
//        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
//
//        new GenericDatumWriter<>(schema).write(record, encoder);
//        encoder.flush();
//        return stream.toByteArray();
//    }
//}
