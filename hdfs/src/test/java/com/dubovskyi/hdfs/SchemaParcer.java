package com.dubovskyi.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;


public class SchemaParcer {


    @Test
    public void schemaCreater(){
        String first = "srch_destination_id";
        String[] names = "d1,d2,d3,d4,d5,d6,d7,d8,d9,d10,d11,d12,d13,d14,d15,d16,d17,d18,d19,d20,d21,d22,d23,d24,d25,d26,d27,d28,d29,d30,d31,d32,d33,d34,d35,d36,d37,d38,d39,d40,d41,d42,d43,d44,d45,d46,d47,d48,d49,d50,d51,d52,d53,d54,d55,d56,d57,d58,d59,d60,d61,d62,d63,d64,d65,d66,d67,d68,d69,d70,d71,d72,d73,d74,d75,d76,d77,d78,d79,d80,d81,d82,d83,d84,d85,d86,d87,d88,d89,d90,d91,d92,d93,d94,d95,d96,d97,d98,d99,d100,d101,d102,d103,d104,d105,d106,d107,d108,d109,d110,d111,d112,d113,d114,d115,d116,d117,d118,d119,d120,d121,d122,d123,d124,d125,d126,d127,d128,d129,d130,d131,d132,d133,d134,d135,d136,d137,d138,d139,d140,d141,d142,d143,d144,d145,d146,d147,d148,d149".split(",");

        System.out.println(Double.parseDouble("-2.18169033283"));

        ///Schema.Field field = new SchemaBuilder.FieldBuilder() //.name(fieldName).type().stringType().noDefault();

    }


    @Test
    public void createSchema(){
      Schema schema =  Application.parseSchema();
      Assertions.assertNotNull(schema);

        Schema.Field field = schema.getField("srch_destination_id");
        Assertions.assertEquals("\"long\"",field.schema().getTypes().get(1).toString(true));

    }

}
