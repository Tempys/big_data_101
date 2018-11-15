package com.dubovskyi.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class SchemaParcerTest {

    @Test
    public void createSchema(){

      Schema schema =  SchemaHelper.parseSchema("destinations.avsc");
      Assertions.assertNotNull(schema);

      Assertions.assertEquals(Schema.Type.LONG,schema.getField("srch_destination_id").schema().getTypes().get(1).getType());
      Assertions.assertEquals(Schema.Type.DOUBLE,schema.getField("d100").schema().getTypes().get(1).getType());

    }

}
