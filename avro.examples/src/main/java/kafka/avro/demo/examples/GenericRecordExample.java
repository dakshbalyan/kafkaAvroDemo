package kafka.avro.demo.examples;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.*;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import javax.xml.validation.SchemaFactoryLoader;
import java.io.File;
import java.io.IOException;

public class GenericRecordExample {
    public static void main(String[] args) {
        // Step 0. Defining the schema
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(" {\n" +
                "     \"type\": \"record\",\n" +
                "     \"namespace\": \"com.example\",\n" +
                "     \"name\": \"Customer\",\n" +
                "     \"doc\" : \"Avro schema for customer\",\n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
                "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
                "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
                "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
                "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
                "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n" +
                "     ]\n" +
                "}");

        // Step 1. Creating a Generic Record
        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
        customerBuilder.set("first_name", "John");
        customerBuilder.set("last_name", "Doe");
        customerBuilder.set("age", 30);
        customerBuilder.set("height", 198f);
        customerBuilder.set("weight", 98f);
        customerBuilder.set("automated_email", false);
        GenericData.Record customer = customerBuilder.build();
        System.out.println(customer);

        GenericRecordBuilder customerBuilderWithDefault = new GenericRecordBuilder(schema);
        customerBuilderWithDefault.set("first_name", "John");
        customerBuilderWithDefault.set("last_name", "Doe");
        customerBuilderWithDefault.set("age", 30);
        customerBuilderWithDefault.set("height", 198f);
        customerBuilderWithDefault.set("weight", 98f);
        GenericData.Record customerWithDefault = customerBuilderWithDefault.build();
        System.out.println(customerWithDefault);

        // Step 2. Writing that generic record to a file
        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-generic.avro"));
//            System.out.println(customer.getClass());
            dataFileWriter.append(customer);
            System.out.println("Written customer-generic.avro");
        } catch (Exception e) {
            System.out.println("Couldn't write file");
            e.printStackTrace();
        }

        // Step 3. Reading a generic record from a file
        final File file = new File("customer-generic.avro");
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        GenericRecord customerRead;
        try (DataFileReader<GenericRecord> dataFileReader = new DataFileReader<>(file, datumReader)){

        // Step 4. interpreting as a generic record
            customerRead = dataFileReader.next();
            System.out.println("Successfully read avro file");
            System.out.println(customerRead.toString());

            // get the data from the generic record
            System.out.println("First name: " + customerRead.get("first_name"));

            // read a non existent field gives us null
            System.out.println("Non existent field: " + customerRead.get("not_here"));
        }
        catch(IOException e) {
            e.printStackTrace();
        }


        // Step 4. interpreting as a generic record
    }
}
