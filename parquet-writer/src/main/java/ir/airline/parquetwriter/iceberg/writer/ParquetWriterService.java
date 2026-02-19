package ir.airline.parquetwriter.iceberg.writer;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class ParquetWriterService {

    public DataFile writeParquet(
            Table table,
            List<Record> records,
            String relativePath,
            PartitionKey partitionKey
    ) throws IOException {

        Schema schema = table.schema();
        OutputFile out = table.io().newOutputFile(
                table.locationProvider().newDataLocation(relativePath)
        );
        DataWriter<Record> writer = Parquet.writeData(out)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .forTable(table)
                .withPartition(partitionKey)
                .build();

        try (writer) {
            for (Record r : records) {
                writer.write(r);
            }
        }

        return writer.toDataFile();
    }

    public static String newParquetFilename() {
        return FileFormat.PARQUET.addExtension(UUID.randomUUID().toString());
    }
}
