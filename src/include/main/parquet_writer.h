#pragma once

#include "arrow/io/file.h"
#include "common/arrow/arrow_converter.h"
#include "parquet/stream_writer.h"

namespace kuzu {
namespace main {

class ParquetWriter {
public:
    static void writeToParquet(QueryResult& queryResult, const std::string& fileName);

private:
    static std::shared_ptr<parquet::WriterProperties> getParquetWriterProperties();
    static std::shared_ptr<parquet::schema::GroupNode> createParquetSchema(
        QueryResult& queryResult);
    static std::shared_ptr<parquet::schema::Node> kuzuTypeToParquetType(
        std::string& columnName, const common::LogicalTypeID& typeID);
    static void streamWrite(parquet::StreamWriter& streamWriter, common::LogicalTypeID& typeID,
        common::Value* resultVal);
};

} // namespace main
} // namespace kuzu
