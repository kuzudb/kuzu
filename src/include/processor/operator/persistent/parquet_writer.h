#pragma once

#include "arrow/io/file.h"
#include "processor/operator/persistent/csv_parquet_writer.h"
#include "processor/operator/persistent/parquet_column_writer.h"
#include <parquet/api/writer.h>

namespace kuzu {
namespace processor {

// ParquetWriter performs the following:
// openFile: opens the file and create a arrow::io::FileOutputStream object
// init:
//   - generate the schema
//   - calculate the max definition levels and number of primitive nodes
//   - initialize parquetColumnWriter
// writeValues : take a vector of ValueVector and pass to parquetColumnWriter
class ParquetWriter : public CSVParquetWriter {
public:
    ParquetWriter(){};
    void openFile(const std::string& filePath) override;
    void init() override;
    void closeFile() override;
    void writeValues(std::vector<common::ValueVector*>& outputVectors) override;

private:
    static std::shared_ptr<parquet::schema::Node> kuzuTypeToParquetType(
        ParquetColumnDescriptor& columnDescriptor, std::string& columnName,
        const common::LogicalType& logicalType,
        parquet::Repetition::type repetition = parquet::Repetition::REQUIRED, int length = -1);

    void writeValue(common::LogicalTypeID type, void* value);

    void flush();

    void generateSchema(std::shared_ptr<parquet::schema::GroupNode>& schema,
        std::unordered_map<int, ParquetColumnDescriptor>& columnDescriptors);

    std::shared_ptr<ParquetColumnWriter> parquetColumnWriter;

    parquet::RowGroupWriter* rowWriter;

    std::shared_ptr<parquet::ParquetFileWriter> fileWriter;

    std::shared_ptr<arrow::io::FileOutputStream> outFile;
};

} // namespace processor
} // namespace kuzu
