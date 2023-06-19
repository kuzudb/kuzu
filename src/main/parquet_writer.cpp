#include "main/parquet_writer.h"

using namespace kuzu::common;

namespace kuzu {
namespace main {

std::shared_ptr<parquet::schema::Node> ParquetWriter::kuzuTypeToParquetType(
    std::string& columnName, const LogicalTypeID& typeID) {
    parquet::Type::type parquetType;
    parquet::ConvertedType::type convertedType;
    switch (typeID) {
    case common::LogicalTypeID::STRING:
        parquetType = parquet::Type::BYTE_ARRAY;
        convertedType = parquet::ConvertedType::UTF8;
        break;
    case common::LogicalTypeID::INT64:
        parquetType = parquet::Type::INT64;
        convertedType = parquet::ConvertedType::INT_64;
        break;
    case common::LogicalTypeID::INT16:
        parquetType = parquet::Type::INT32;
        convertedType = parquet::ConvertedType::INT_16;
        break;
    case common::LogicalTypeID::INT32:
        parquetType = parquet::Type::INT32;
        convertedType = parquet::ConvertedType::INT_32;
        break;
    case common::LogicalTypeID::FLOAT:
        parquetType = parquet::Type::FLOAT;
        convertedType = parquet::ConvertedType::NONE;
        break;
    case common::LogicalTypeID::DOUBLE:
        parquetType = parquet::Type::DOUBLE;
        convertedType = parquet::ConvertedType::NONE;
        break;
    case common::LogicalTypeID::DATE:
        parquetType = parquet::Type::BYTE_ARRAY;
        convertedType = parquet::ConvertedType::NONE;
        break;
    case common::LogicalTypeID::TIMESTAMP:
        parquetType = parquet::Type::BYTE_ARRAY;
        convertedType = parquet::ConvertedType::TIMESTAMP_MILLIS;
        break;
    case common::LogicalTypeID::INTERVAL:
        parquetType = parquet::Type::BYTE_ARRAY;
        convertedType = parquet::ConvertedType::INTERVAL;
        break;
    default:
        throw RuntimeException("Unsupported type for Parquet datatype");
    }
    return parquet::schema::PrimitiveNode::Make(
        columnName, parquet::Repetition::REQUIRED, parquetType, convertedType);
}

std::shared_ptr<parquet::schema::GroupNode> ParquetWriter::createParquetSchema(
    QueryResult& queryResult) {
    std::vector<std::shared_ptr<parquet::schema::Node>> fields;
    auto typesInfo = queryResult.getColumnTypesInfo();
    auto columnNames = queryResult.getColumnNames();
    for (auto i = 0u; i < typesInfo.size(); ++i) {
        fields.push_back(kuzuTypeToParquetType(columnNames[i], typesInfo[i]->typeID));
    }
    return std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
}

void ParquetWriter::streamWrite(
    parquet::StreamWriter& streamWriter, LogicalTypeID& typeID, Value* resultVal) {
    switch (typeID) {
    case common::LogicalTypeID::INT64:
        streamWriter << resultVal->getValue<int64_t>();
        break;
    case common::LogicalTypeID::INT16:
        streamWriter << resultVal->getValue<int16_t>();
        break;
    case common::LogicalTypeID::INT32:
        streamWriter << resultVal->getValue<int32_t>();
        break;
    case common::LogicalTypeID::FLOAT:
        streamWriter << resultVal->getValue<float>();
        break;
    case common::LogicalTypeID::DOUBLE:
        streamWriter << resultVal->getValue<double>();
        break;
        //        case common::LogicalTypeID::DATE:
        //            streamWriter << resultVal->getValue<date_t>();
        //        break;
        //        case common::LogicalTypeID::TIMESTAMP:
        //            streamWriter << resultVal->getValue<timestamp_t>();
        //        break;
        //        case common::LogicalTypeID::INTERVAL:
        //            streamWriter << resultVal->getValue<interval_t>();
        //        break;
    default:
        streamWriter << resultVal->getValue<std::string>();
        break;
    }
}

std::shared_ptr<parquet::WriterProperties> ParquetWriter::getParquetWriterProperties() {
    std::shared_ptr<parquet::WriterProperties> props =
        parquet::WriterProperties::Builder().compression(arrow::Compression::SNAPPY)->build();
    return props;
}

void ParquetWriter::writeToParquet(QueryResult& queryResult, const std::string& fileName) {
    auto outputStream = arrow::io::FileOutputStream::Open(fileName);
    if (!outputStream.ok()) {
        throw RuntimeException("Could not open file for writing");
    }
    auto props = getParquetWriterProperties();
    auto schema = createParquetSchema(queryResult);
    parquet::StreamWriter streamWriter{
        parquet::ParquetFileWriter::Open(*outputStream, schema, props)};
    std::shared_ptr<processor::FlatTuple> nextTuple;
    while (queryResult.hasNext()) {
        nextTuple = queryResult.getNext();
        for (auto idx = 0ul; idx < nextTuple->len(); idx++) {
            auto resultVal = nextTuple->getValue(idx);
            auto resultDataType = nextTuple->getValue(idx)->getDataType().getLogicalTypeID();
            streamWrite(streamWriter, resultDataType, resultVal);
        }
        streamWriter << parquet::EndRow;
    }
}

} // namespace main
} // namespace kuzu
