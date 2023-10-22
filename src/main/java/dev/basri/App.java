package dev.basri;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;

import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;

public class App {

    public Schema readSchema(String classpathSchemaFilePath) {
        if (classpathSchemaFilePath == null) {
            throw new IllegalArgumentException();
        }

        String schemaJson;
        try {
            Paths.get(classpathSchemaFilePath);

            schemaJson = Files.readString(
                    Paths.get(classpathSchemaFilePath), Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Cannot read schema file: " + classpathSchemaFilePath, e);
        }

        Schema.Parser parser = new Schema.Parser().setValidate(true);
        return parser.parse(schemaJson);
    }

    List<GenericData.Record> createRecords(Schema schema) {
        List<GenericData.Record> records = new ArrayList<>();

        GenericData.Record record1 = new GenericData.Record(schema);
        record1.put("name", "basri");
        record1.put("surname", "kahveci");
        record1.put("birthYear", 1989);
        record1.put("country", "uk");

        records.add(record1);

        GenericData.Record record2 = new GenericData.Record(schema);
        record2.put("name", "fatma");
        record2.put("surname", "kahveci");
        record2.put("birthYear", 1988);
        record2.put("country", "uk");

        records.add(record2);

        return records;
    }

    private void writeParquetFile(String classpathSchemaFilePath, String outputFilePath) {
        Schema schema = readSchema(classpathSchemaFilePath);
        List<GenericData.Record> recordList = createRecords(schema);
        Configuration hadoopConfiguration = new Configuration();

        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter.<GenericData.Record>builder(
                HadoopOutputFile.fromPath(new Path(outputFilePath), hadoopConfiguration))
                .withSchema(schema)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withRowGroupSize(ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(ParquetWriter.DEFAULT_PAGE_SIZE)
                .withConf(hadoopConfiguration)
                .withValidation(false)
                .withDictionaryEncoding(true)
                .build()) {

            for (GenericData.Record record : recordList) {
                writer.write(record);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write parquet file!", e);
        }
    }

    public void readParquetFile(String outputFilePath) {
        Configuration hadoopConfiguration = new Configuration();
        try (ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(
                HadoopInputFile.fromPath(new Path(outputFilePath), hadoopConfiguration)).withConf(hadoopConfiguration)
                .build()) {
            GenericData.Record next;
            while ((next = reader.read()) != null) {
                System.out.println("Read row: " + next);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write parquet file!", e);
        }
    }

    public void readParquetData(String filePath) {
        List<SimpleGroup> simpleGroups = new ArrayList<>();
        try (ParquetFileReader reader = ParquetFileReader
                .open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));) {
            MessageType schema = reader.getFooter().getFileMetaData().getSchema();
            System.out.println("Schema: " + schema);
            PageReadStore pages;
            while ((pages = reader.readNextRowGroup()) != null) {
                long rows = pages.getRowCount();
                MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                for (int i = 0; i < rows; i++) {
                    SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
                    simpleGroups.add(simpleGroup);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to read parquet file!", e);
        }

        for (SimpleGroup group : simpleGroups) {
            System.out.println(group);
        }
    }

    public static void main(String[] args) {
        System.out.println("Hello World!");

        String classPathSchemaPath = "people_schema.json";
        String outputFileName = "data_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd-HHmm"))
                + ".parquet";

        App app = new App();
        // app.writeParquetFile(classPathSchemaPath, outputFileName);
        app.readParquetData("data_20231022-0930.parquet");
    }
}
