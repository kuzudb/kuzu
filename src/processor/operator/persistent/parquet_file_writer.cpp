#include "processor/operator/persistent/parquet_file_writer.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void ParquetFileWriter::openFile() {
    auto result = arrow::io::FileOutputStream::Open(filePath);
    if (!result.ok()) {
        throw RuntimeException(result.status().ToString());
    }
    outFile = *result;
}

void ParquetFileWriter::init() {
    std::shared_ptr<parquet::schema::GroupNode> schema;
    std::vector<int> maxDefinitionLevels;
    openFile();
    // A nested Kuzu column may be represented by multiple parquet columns
    // so we need this information for writing, that is generated inside createParquetSchemaNode
    int parquetColumnsCount = 0;
    generateSchema(schema, parquetColumnsCount);
    parquet::WriterProperties::Builder builder;
    builder.compression(parquet::Compression::SNAPPY);
    std::shared_ptr<parquet::WriterProperties> props = builder.build();
    fileWriter = parquet::ParquetFileWriter::Open(outFile, schema, props);
    const parquet::SchemaDescriptor* schemaDescriptor = fileWriter->schema();
    for (auto i = 0u; i < columnNames.size(); ++i) {
        const parquet::ColumnDescriptor* colDesc = schemaDescriptor->Column(i);
        maxDefinitionLevels.push_back(colDesc->max_definition_level());
    }
    parquetColumnWriter =
        std::make_shared<ParquetColumnWriter>(parquetColumnsCount, maxDefinitionLevels);
    rowGroupWriter = fileWriter->AppendBufferedRowGroup();
}

void ParquetFileWriter::generateSchema(
    std::shared_ptr<parquet::schema::GroupNode>& schema, int& parquetColumnsCount) {
    parquet::schema::NodeVector fields;
    for (auto i = 0u; i < columnNames.size(); ++i) {
        fields.push_back(
            createParquetSchemaNode(parquetColumnsCount, columnNames[i], columnTypes[i]));
    }
    schema = std::static_pointer_cast<parquet::schema::GroupNode>(
        parquet::schema::GroupNode::Make("schema", parquet::Repetition::REQUIRED, fields));
}

// Return a parquet::schema::Node, which contains the schema for a single Kuzu
// column. The node can be a primitive node or a group node.
// A primitive node schema is like:
//
// required group field_id=-1 schema {
//  required binary field_id=-1 single string (String);
// }
//
// Parquet Physical Types:
//
// BOOLEAN: 1 bit boolean
// INT32: 32 bit signed ints
// INT64: 64 bit signed ints
// INT96: 96 bit signed ints
// FLOAT: IEEE 32-bit floating point values
// DOUBLE: IEEE 64-bit floating point values
// BYTE_ARRAY: arbitrarily long byte arrays.
// https://github.com/apache/parquet-cpp/blob/master/src/parquet/column_writer.h

std::shared_ptr<parquet::schema::Node> ParquetFileWriter::createPrimitiveNode(
    int& parquetColumnsCount, std::string& columnName, const LogicalType& logicalType,
    parquet::Repetition::type repetition, int length) {
    parquet::Type::type parquetType;
    parquet::ConvertedType::type convertedType = parquet::ConvertedType::NONE;
    parquetColumnsCount++;
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        parquetType = parquet::Type::BOOLEAN;
    } break;
    case LogicalTypeID::INTERNAL_ID:
    case LogicalTypeID::STRING: {
        parquetType = parquet::Type::BYTE_ARRAY;
        convertedType = parquet::ConvertedType::UTF8;
    } break;
    case LogicalTypeID::INT64: {
        parquetType = parquet::Type::INT64;
        convertedType = parquet::ConvertedType::INT_64;
    } break;
    case LogicalTypeID::INT16: {
        parquetType = parquet::Type::INT32;
        convertedType = parquet::ConvertedType::INT_16;
    } break;
    case LogicalTypeID::INT32: {
        parquetType = parquet::Type::INT32;
        convertedType = parquet::ConvertedType::INT_32;
    } break;
    case LogicalTypeID::FLOAT: {
        parquetType = parquet::Type::FLOAT;
    } break;
    case LogicalTypeID::DOUBLE: {
        parquetType = parquet::Type::DOUBLE;
    } break;
    case LogicalTypeID::DATE: {
        parquetType = parquet::Type::INT32;
        convertedType = parquet::ConvertedType::DATE;
    } break;
    case LogicalTypeID::TIMESTAMP: {
        parquetType = parquet::Type::INT64;
        convertedType = parquet::ConvertedType::TIMESTAMP_MICROS;
    } break;
    case LogicalTypeID::INTERVAL: {
        parquetType = parquet::Type::FIXED_LEN_BYTE_ARRAY;
        convertedType = parquet::ConvertedType::INTERVAL;
        length = 12;
    } break;
    default:
        throw NotImplementedException("ParquetFileWriter::createPrimitiveNode");
    }
    return parquet::schema::PrimitiveNode::Make(
        columnName, repetition, parquetType, convertedType, length);
}

std::shared_ptr<parquet::schema::Node> ParquetFileWriter::createNestedNode(
    int& parquetColumnsCount, std::string& columnName, const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::STRUCT: {
        auto structType = StructType::getFieldTypes(&logicalType);
        auto structNames = StructType::getFieldNames(&logicalType);
        std::vector<std::shared_ptr<parquet::schema::Node>> nodes;
        for (auto i = 0u; i < structType.size(); ++i) {
            nodes.push_back(createParquetSchemaNode(parquetColumnsCount, structNames[i],
                *structType[i], parquet::Repetition::OPTIONAL));
        }
        auto groupNode = std::static_pointer_cast<parquet::schema::GroupNode>(
            parquet::schema::GroupNode::Make(columnName, parquet::Repetition::OPTIONAL, nodes));
        return groupNode;
    }
    //   A list is represented by the following schema:
    //   required group field_id=-1 schema {
    //     optional group field_id=-1 list (List) {
    //       repeated group field_id=-1  {
    //         optional int64 field_id=-1 item;
    //       }
    //     }
    //   }
    //
    //   A nested list is encapsulated by an optional + repeated groups:
    //    required group field_id=-1 schema {
    //      optional group field_id=-1 list (List) {
    //        repeated group field_id=-1  {
    //          optional group field_id=-1  (List) {
    //            repeated group field_id=-1  {
    //              optional int64 field_id=-1 item;
    //            }
    //          }
    //        }
    //      }
    //    }
    case LogicalTypeID::FIXED_LIST:
    case LogicalTypeID::VAR_LIST: {
        auto childLogicalType = VarListType::getChildType(&logicalType);
        auto childNode = createParquetSchemaNode(
            parquetColumnsCount, columnName, *childLogicalType, parquet::Repetition::OPTIONAL);
        auto repeatedGroup =
            parquet::schema::GroupNode::Make("", parquet::Repetition::REPEATED, {childNode});
        auto optional = parquet::schema::GroupNode::Make(columnName, parquet::Repetition::OPTIONAL,
            {repeatedGroup}, parquet::LogicalType::List());
        return optional;
    }
    default:
        throw NotImplementedException("ParquetFileWriter::createParquetSchemaNode");
    }
}

std::shared_ptr<parquet::schema::Node> ParquetFileWriter::createParquetSchemaNode(
    int& parquetColumnsCount, std::string& columnName, const LogicalType& logicalType,
    parquet::Repetition::type repetition, int length) {
    if (LogicalTypeUtils::isNested(logicalType)) {
        return createNestedNode(parquetColumnsCount, columnName, logicalType);
    } else {
        return createPrimitiveNode(
            parquetColumnsCount, columnName, logicalType, repetition, length);
    }
}

void ParquetFileWriter::writeValues(std::vector<ValueVector*>& outputVectors) {
    if (outputVectors.size() == 0) {
        return;
    }
    for (auto i = 0u; i < outputVectors.size(); i++) {
        assert(outputVectors[i]->state->isFlat());
        parquetColumnWriter->writeColumn(i, outputVectors[i], rowGroupWriter);
    }
    if ((rowGroupWriter->total_bytes_written() + rowGroupWriter->total_compressed_bytes() +
            parquetColumnWriter->estimatedRowBytes) > StorageConstants::NODE_GROUP_SIZE) {
        rowGroupWriter->Close();
        rowGroupWriter = fileWriter->AppendBufferedRowGroup();
        parquetColumnWriter->estimatedRowBytes = 0;
    }
}

void ParquetFileWriter::closeFile() {
    rowGroupWriter->Close();
    fileWriter->Close();
    auto status = outFile->Close();
    if (!status.ok()) {
        throw RuntimeException("Error closing file in ParquetFileWriter.");
    }
}

} // namespace processor
} // namespace kuzu
