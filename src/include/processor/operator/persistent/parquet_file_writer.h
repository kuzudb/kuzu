#pragma once

#include "arrow/io/file.h"
#include "processor/operator/persistent/file_writer.h"
#include "processor/operator/persistent/parquet_column_writer.h"
#include <parquet/api/writer.h>

namespace kuzu {
namespace processor {

// ParquetFileWriter performs the following:
// openFile: opens the file and create an arrow::io::FileOutputStream object
// init:
//   - generate the schema
//   - calculate the max definition levels and number of primitive nodes. The
//     definition levels are used for nested types.
//   - initialize parquetColumnWriter
// writeValues : take a vector of ValueVector and pass to parquetColumnWriter
// More information about the encoding:
// https://parquet.apache.org/docs/file-format/data-pages/encodings
class ParquetFileWriter : public FileWriter {
public:
    using FileWriter::FileWriter;
    void openFile() final;
    void init() final;
    void closeFile() final;
    void writeValues(std::vector<common::ValueVector*>& outputVectors) final;

private:
    static std::shared_ptr<parquet::schema::Node> createParquetSchemaNode(int& parquetColumnsCount,
        std::string& columnName, const common::LogicalType& logicalType,
        parquet::Repetition::type repetition = parquet::Repetition::REQUIRED, int length = -1);

    static std::shared_ptr<parquet::schema::Node> createNestedNode(
        int& parquetColumnsCount, std::string& columnName, const common::LogicalType& logicalType);

    static std::shared_ptr<parquet::schema::Node> createPrimitiveNode(int& parquetColumnsCount,
        std::string& columnName, const common::LogicalType& logicalType,
        parquet::Repetition::type repetition, int length);

    void writeValue(common::LogicalTypeID type, void* value);

    void flush();

    void generateSchema(
        std::shared_ptr<parquet::schema::GroupNode>& schema, int& parquetColumnsCount);

    std::shared_ptr<ParquetColumnWriter> parquetColumnWriter;

    parquet::RowGroupWriter* rowGroupWriter;

    std::shared_ptr<parquet::ParquetFileWriter> fileWriter;

    std::shared_ptr<arrow::io::FileOutputStream> outFile;
};

} // namespace processor
} // namespace kuzu
