package ir.airline.parquetwriter.iceberg.writer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import lombok.RequiredArgsConstructor;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileFormat;
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

    public List<DataFile> writeParquet(Table table, List<Record> records) throws IOException {
        var schema = table.schema();
        List<DataFile> files = new ArrayList<>();

        String filename = FileFormat.PARQUET.addExtension(UUID.randomUUID().toString());
        OutputFile out = table.io().newOutputFile(table.locationProvider().newDataLocation(filename));

        DataWriter<Record> writer = Parquet.writeData(out)
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .forTable(table)
                .build();
        try (writer) {
            for (Record r : records) {
                writer.write(r);
            }
        }

        files.add(writer.toDataFile());
        return files;
    }
}
