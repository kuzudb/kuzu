#include "processor/operator/persistent/writer/parquet/column_writer.h"

#include "common/exception/not_implemented.h"
#include "common/string_utils.h"
#include "function/cast/functions/numeric_limits.h"
#include "processor/operator/persistent/writer/parquet/boolean_column_writer.h"
#include "processor/operator/persistent/writer/parquet/parquet_writer.h"
#include "processor/operator/persistent/writer/parquet/standard_column_writer.h"
#include "processor/operator/persistent/writer/parquet/string_column_writer.h"
#include "processor/operator/persistent/writer/parquet/struct_column_writer.h"
#include "processor/operator/persistent/writer/parquet/var_list_column_writer.h"
#include "snappy/snappy.h"

namespace kuzu {
namespace processor {

using namespace kuzu_parquet::format;
using namespace kuzu::common;

ColumnWriter::ColumnWriter(ParquetWriter& writer, uint64_t schemaIdx,
    std::vector<std::string> schemaPath, uint64_t maxRepeat, uint64_t maxDefine, bool canHaveNulls)
    : writer{writer}, schemaIdx{schemaIdx}, schemaPath{std::move(schemaPath)}, maxRepeat{maxRepeat},
      maxDefine{maxDefine}, canHaveNulls{canHaveNulls}, nullCount{0} {}

std::unique_ptr<ColumnWriter> ColumnWriter::createWriterRecursive(
    std::vector<kuzu_parquet::format::SchemaElement>& schemas, ParquetWriter& writer,
    LogicalType* type, const std::string& name, std::vector<std::string> schemaPathToCreate,
    storage::MemoryManager* mm, uint64_t maxRepeatToCreate, uint64_t maxDefineToCreate,
    bool canHaveNullsToCreate) {
    auto nullType =
        canHaveNullsToCreate ? FieldRepetitionType::OPTIONAL : FieldRepetitionType::REQUIRED;
    if (!canHaveNullsToCreate) {
        maxDefineToCreate--;
    }
    auto schemaIdx = schemas.size();
    switch (type->getLogicalTypeID()) {
    case LogicalTypeID::STRUCT: {
        auto fields = StructType::getFields(type);
        // set up the schema element for this struct
        kuzu_parquet::format::SchemaElement schema_element;
        schema_element.repetition_type = nullType;
        schema_element.num_children = fields.size();
        schema_element.__isset.num_children = true;
        schema_element.__isset.type = false;
        schema_element.__isset.repetition_type = true;
        schema_element.name = name;
        schemas.push_back(std::move(schema_element));
        schemaPathToCreate.push_back(name);

        // construct the child types recursively
        std::vector<std::unique_ptr<ColumnWriter>> childWriters;
        childWriters.reserve(fields.size());
        for (auto& field : fields) {
            childWriters.push_back(
                createWriterRecursive(schemas, writer, field->getType(), field->getName(),
                    schemaPathToCreate, mm, maxRepeatToCreate, maxDefineToCreate + 1));
        }
        return std::make_unique<StructColumnWriter>(writer, schemaIdx,
            std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
            std::move(childWriters), canHaveNullsToCreate);
    }
    case LogicalTypeID::VAR_LIST: {
        auto childType = VarListType::getChildType(type);
        // set up the two schema elements for the list
        // for some reason we only set the converted type in the OPTIONAL element
        // first an OPTIONAL element
        kuzu_parquet::format::SchemaElement optional_element;
        optional_element.repetition_type = nullType;
        optional_element.num_children = 1;
        optional_element.converted_type = ConvertedType::LIST;
        optional_element.__isset.num_children = true;
        optional_element.__isset.type = false;
        optional_element.__isset.repetition_type = true;
        optional_element.__isset.converted_type = true;
        optional_element.name = name;
        schemas.push_back(std::move(optional_element));
        schemaPathToCreate.push_back(name);

        // then a REPEATED element
        kuzu_parquet::format::SchemaElement repeated_element;
        repeated_element.repetition_type = FieldRepetitionType::REPEATED;
        repeated_element.num_children = 1;
        repeated_element.__isset.num_children = true;
        repeated_element.__isset.type = false;
        repeated_element.__isset.repetition_type = true;
        repeated_element.name = "list";
        schemas.push_back(std::move(repeated_element));
        schemaPathToCreate.emplace_back("list");

        auto child_writer = createWriterRecursive(schemas, writer, childType, "element",
            schemaPathToCreate, mm, maxRepeatToCreate + 1, maxDefineToCreate + 2);
        return std::make_unique<VarListColumnWriter>(writer, schemaIdx,
            std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
            std::move(child_writer), canHaveNullsToCreate);
    }
    default: {
        SchemaElement schemaElement;
        schemaElement.type = ParquetWriter::convertToParquetType(type);
        schemaElement.repetition_type = nullType;
        schemaElement.__isset.num_children = false;
        schemaElement.__isset.type = true;
        schemaElement.__isset.repetition_type = true;
        schemaElement.name = name;
        ParquetWriter::setSchemaProperties(type, schemaElement);
        schemas.push_back(std::move(schemaElement));
        schemaPathToCreate.push_back(name);

        switch (type->getLogicalTypeID()) {
        case LogicalTypeID::BOOL:
            return std::make_unique<BooleanColumnWriter>(writer, schemaIdx,
                std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
                canHaveNullsToCreate);
        case LogicalTypeID::INT16:
            return std::make_unique<StandardColumnWriter<int16_t, int32_t>>(writer, schemaIdx,
                std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
                canHaveNullsToCreate);
        case LogicalTypeID::INT32:
        case LogicalTypeID::DATE:
            return std::make_unique<StandardColumnWriter<int32_t, int32_t>>(writer, schemaIdx,
                std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
                canHaveNullsToCreate);
        case LogicalTypeID::INT64:
            return std::make_unique<StandardColumnWriter<int64_t, int64_t>>(writer, schemaIdx,
                std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
                canHaveNullsToCreate);
        case LogicalTypeID::FLOAT:
            return std::make_unique<StandardColumnWriter<float, float>>(writer, schemaIdx,
                std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
                canHaveNullsToCreate);
        case LogicalTypeID::DOUBLE:
            return std::make_unique<StandardColumnWriter<double, double>>(writer, schemaIdx,
                std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
                canHaveNullsToCreate);
        case LogicalTypeID::BLOB:
        case LogicalTypeID::STRING:
            return std::make_unique<StringColumnWriter>(writer, schemaIdx,
                std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
                canHaveNullsToCreate, mm);
        default:
            throw NotImplementedException("ParquetWriter::convertToParquetType");
        }
    }
    }
}

void ColumnWriter::handleRepeatLevels(ColumnWriterState& stateToHandle, ColumnWriterState* parent) {
    if (!parent) {
        // no repeat levels without a parent node
        return;
    }
    while (stateToHandle.repetitionLevels.size() < parent->repetitionLevels.size()) {
        stateToHandle.repetitionLevels.push_back(
            parent->repetitionLevels[stateToHandle.repetitionLevels.size()]);
    }
}

void ColumnWriter::handleDefineLevels(ColumnWriterState& state, ColumnWriterState* parent,
    common::ValueVector* vector, uint64_t count, uint16_t defineValue, uint16_t nullValue) {
    if (parent) {
        // parent node: inherit definition level from the parent
        uint64_t vectorIdx = 0;
        while (state.definitionLevels.size() < parent->definitionLevels.size()) {
            auto currentIdx = state.definitionLevels.size();
            if (parent->definitionLevels[currentIdx] != ParquetConstants::PARQUET_DEFINE_VALID) {
                state.definitionLevels.push_back(parent->definitionLevels[currentIdx]);
            } else if (!vector->isNull(getVectorPos(vector, vectorIdx))) {
                state.definitionLevels.push_back(defineValue);
            } else {
                if (!canHaveNulls) {
                    throw RuntimeException(
                        "Parquet writer: map key column is not allowed to contain NULL values");
                }
                nullCount++;
                state.definitionLevels.push_back(nullValue);
            }
            if (parent->isEmpty.empty() || !parent->isEmpty[currentIdx]) {
                vectorIdx++;
            }
        }
    } else {
        // no parent: set definition levels only from this validity mask
        for (auto i = 0u; i < count; i++) {
            if (!vector->isNull(getVectorPos(vector, i))) {
                state.definitionLevels.push_back(defineValue);
            } else {
                if (!canHaveNulls) {
                    throw RuntimeException(
                        "Parquet writer: map key column is not allowed to contain NULL values");
                }
                nullCount++;
                state.definitionLevels.push_back(nullValue);
            }
        }
    }
}

void ColumnWriter::compressPage(common::BufferedSerializer& bufferedSerializer,
    size_t& compressedSize, uint8_t*& compressedData, std::unique_ptr<uint8_t[]>& compressedBuf) {
    switch (writer.getCodec()) {
    case CompressionCodec::UNCOMPRESSED: {
        compressedSize = bufferedSerializer.getSize();
        compressedData = bufferedSerializer.getBlobData();
    } break;
    case CompressionCodec::SNAPPY: {
        compressedSize = kuzu_snappy::MaxCompressedLength(bufferedSerializer.getSize());
        compressedBuf = std::unique_ptr<uint8_t[]>(new uint8_t[compressedSize]);
        kuzu_snappy::RawCompress(reinterpret_cast<const char*>(bufferedSerializer.getBlobData()),
            bufferedSerializer.getSize(), reinterpret_cast<char*>(compressedBuf.get()),
            &compressedSize);
        compressedData = compressedBuf.get();
        assert(compressedSize <= kuzu_snappy::MaxCompressedLength(bufferedSerializer.getSize()));
    } break;
    default:
        throw NotImplementedException{"ColumnWriter::compressPage"};
    }

    if (compressedSize > uint64_t(function::NumericLimits<int32_t>::maximum())) {
        throw RuntimeException(
            stringFormat("Parquet writer: {} compressed page size out of range for type integer",
                bufferedSerializer.getSize()));
    }
}

} // namespace processor
} // namespace kuzu
