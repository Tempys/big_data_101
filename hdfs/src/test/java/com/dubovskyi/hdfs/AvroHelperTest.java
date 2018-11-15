package com.dubovskyi.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class AvroHelperTest {

    @Test
    public void createSchema(){

      Schema schema =  AvroHelper.parseSchema("destinations.avsc");
      Assertions.assertNotNull(schema);

      assertEquals(Schema.Type.LONG,schema.getField("srch_destination_id").schema().getTypes().get(1).getType());
      assertEquals(Schema.Type.DOUBLE,schema.getField("d100").schema().getTypes().get(1).getType());

    }

    @Test
    public void generateGenericObject(){
        Schema schema =  AvroHelper.parseSchema("destinations.avsc");
        String[] names = new String[]{"srch_destination_id","d1"};
        String line = "5,2.22";

        GenericRecord genericRecord = AvroHelper.createGenericRecord(line,names,schema);

        assertEquals(2.22,genericRecord.get("d1"));
        assertEquals(5L,genericRecord.get("srch_destination_id"));


    }

}
