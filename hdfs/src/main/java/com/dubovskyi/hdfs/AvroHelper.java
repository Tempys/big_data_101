package com.dubovskyi.hdfs;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

public class AvroHelper {


    /*
     *  generate the avro schema object from file.avsc
     */
    public static Schema parseSchema(String path) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try {
            // Path to schema file
            schema = parser.parse(ClassLoader.getSystemResourceAsStream(path));

        } catch (IOException e) {
            e.printStackTrace();
        }
        return schema;
    }

    /*
     * create genericRecord
     * line: scv line from scv file
     * names: columns of scv file
     * schema: Avro schmemas
     */
    public static GenericRecord createGenericRecord(String line, String[] names, Schema schema) {
        GenericRecord record = new GenericData.Record(schema);

        String[] values = line.split(",");


        for(int i =0;i<values.length;i++){
           // System.out.println("name: "+names[i]+ " values: "+ values[i] );
            Object value = toAvro(names[i],values[i],schema);
            record.put(names[i],value);
        }

        return record;
    }

    private static Object toAvro(String name,String value,Schema schema)   {

        Schema.Type type = schema.getField(name).schema().getTypes().get(1).getType();

        switch (type){

            case LONG:  return Long.parseLong(value);
            case INT:   return Integer.parseInt(value);
            case DOUBLE: return value.equals("") ? 0d : Double.parseDouble(value);
            case STRING: return  value;
            default: throw new IllegalArgumentException(String.format("mapping for this type %s is missing",type));

        }

    }

}
