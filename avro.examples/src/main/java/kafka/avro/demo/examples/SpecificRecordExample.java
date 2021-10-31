package kafka.avro.demo.examples;


import com.example.Customer;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SpecificRecordExample {
    public static void main(String[] args) {

        // Step 1. Create Specific Record
        Customer.Builder customerBuilder = Customer.newBuilder();
        customerBuilder.setAge(28);
        customerBuilder.setFirstName("Specific");
        customerBuilder.setLastName("Record");
        customerBuilder.setHeight(190f);
        customerBuilder.setWeight(45f);
        customerBuilder.setAutomatedEmail(false);

        Customer customer = customerBuilder.build();
        System.out.println(customer);

        Customer.Builder customerBuilderWtihDefault = Customer.newBuilder();
        customerBuilderWtihDefault.setAge(36);
        customerBuilderWtihDefault.setFirstName("John");
        customerBuilderWtihDefault.setLastName("Doe");
        customerBuilderWtihDefault.setHeight(179f);
        customerBuilderWtihDefault.setWeight(75f);

        Customer customerWithDefault = customerBuilderWtihDefault.build();
        System.out.println(customerWithDefault);

        // Step 2. Writing specific record to a file
        final DatumWriter<Customer> datumWriter = new SpecificDatumWriter<>(Customer.class);

        try (DataFileWriter<Customer> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(customer.getSchema(), new File("customer-specific.avro"));
            dataFileWriter.append(customer);
            dataFileWriter.append(customerWithDefault);
            System.out.println("successfully wrote customer-specific.avro");
        } catch (IOException e){
            e.printStackTrace();
        }

        // Step 3. Reading the specific record from the file

        final File file = new File("customer-specific.avro");
        final DatumReader<Customer> datumReader = new SpecificDatumReader<>(Customer.class);
        final DataFileReader<Customer> dataFileReader;
        try {
            System.out.println("Reading our specific record");
            dataFileReader = new DataFileReader<>(file, datumReader);
            while (dataFileReader.hasNext()) {
                Customer readCustomer = dataFileReader.next();
        // Step 4. Interpret as a specific record

                System.out.println(readCustomer.toString());
//                System.out.println("First name: " + readCustomer.getFirstName());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // note, we can read our other customer generated using the generic method!
        // end of the day, no matter the method, Avro is Avro!
        final File fileGeneric = new File("customer-generic.avro");
        final DatumReader<Customer> datumReaderGeneric = new SpecificDatumReader<>(Customer.class);
        final DataFileReader<Customer> dataFileReaderGeneric;
        try {
            System.out.println("Reading our specific record");
            dataFileReaderGeneric = new DataFileReader<>(fileGeneric, datumReaderGeneric);
            while (dataFileReaderGeneric.hasNext()) {
                Customer readCustomer = dataFileReaderGeneric.next();
                System.out.println(readCustomer.toString());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
