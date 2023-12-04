#include "processor/operator/persistent/writer/parquet/column_writer.h"

#include "common/string_format.h"
#include "function/cast/functions/numeric_limits.h"
#include "processor/operator/persistent/writer/parquet/boolean_column_writer.h"
#include "processor/operator/persistent/writer/parquet/interval_column_writer.h"
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

struct ParquetInt128Operator {
    template<class SRC, class TGT>
    static inline TGT Operation(SRC input) {
        return Int128_t::Cast<double>(input);
    }

    template<class /*SRC*/, class /*TGT*/>
    static inline std::unique_ptr<ColumnWriterStatistics> initializeStats() {
        return std::make_unique<ColumnWriterStatistics>();
    }

    template<class SRC, class TGT>
    static void handleStats(ColumnWriterStatistics* /*stats*/, SRC /*source*/, TGT /*target*/) {}
};

struct ParquetTimestampNSOperator : public BaseParquetOperator {
    template<class SRC, class TGT>
    static TGT Operation(SRC input) {
        return Timestamp::fromEpochNanoSeconds(input).value;
    }
};

struct ParquetTimestampSOperator : public BaseParquetOperator {
    template<class SRC, class TGT>
    static TGT Operation(SRC input) {
        return Timestamp::fromEpochSeconds(input).value;
    }
};

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
    case LogicalTypeID::UNION:
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

        // Construct the child types recursively.
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
        // Set up the two schema elements for the list
        // for some reason we only set the converted type in the OPTIONAL element
        // first an OPTIONAL element.
        kuzu_parquet::format::SchemaElement optionalElem;
        optionalElem.repetition_type = nullType;
        optionalElem.num_children = 1;
        optionalElem.converted_type = ConvertedType::LIST;
        optionalElem.__isset.num_children = true;
        optionalElem.__isset.type = false;
        optionalElem.__isset.repetition_type = true;
        optionalElem.__isset.converted_type = true;
        optionalElem.name = name;
        schemas.push_back(std::move(optionalElem));
        schemaPathToCreate.push_back(name);

        // Then a REPEATED element.
        kuzu_parquet::format::SchemaElement repeatedElem;
        repeatedElem.repetition_type = FieldRepetitionType::REPEATED;
        repeatedElem.num_children = 1;
        repeatedElem.__isset.num_children = true;
        repeatedElem.__isset.type = false;
        repeatedElem.__isset.repetition_type = true;
        repeatedElem.name = "list";
        schemas.push_back(std::move(repeatedElem));
        schemaPathToCreate.emplace_back("list");

        auto child_writer = createWriterRecursive(schemas, writer, childType, "element",
            schemaPathToCreate, mm, maxRepeatToCreate + 1, maxDefineToCreate + 2);
        return std::make_unique<VarListColumnWriter>(writer, schemaIdx,
            std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
            std::move(child_writer), canHaveNullsToCreate);
    }
    case LogicalTypeID::MAP: {
        // Maps are stored as follows in parquet:
        // <map-repetition> group <name> (MAP) {
        // 	repeated group key_value {
        // 		required <key-type> key;
        // 		<value-repetition> <value-type> value;
        // 	}
        // }
        kuzu_parquet::format::SchemaElement topElement;
        topElement.repetition_type = nullType;
        topElement.num_children = 1;
        topElement.converted_type = ConvertedType::MAP;
        topElement.__isset.repetition_type = true;
        topElement.__isset.num_children = true;
        topElement.__isset.converted_type = true;
        topElement.__isset.type = false;
        topElement.name = name;
        schemas.push_back(std::move(topElement));
        schemaPathToCreate.push_back(name);

        // key_value element
        kuzu_parquet::format::SchemaElement kv_element;
        kv_element.repetition_type = FieldRepetitionType::REPEATED;
        kv_element.num_children = 2;
        kv_element.__isset.repetition_type = true;
        kv_element.__isset.num_children = true;
        kv_element.__isset.type = false;
        kv_element.name = "key_value";
        schemas.push_back(std::move(kv_element));
        schemaPathToCreate.emplace_back("key_value");

        // Construct the child types recursively.
        std::vector<common::LogicalType*> kvTypes{
            MapType::getKeyType(type), MapType::getValueType(type)};
        std::vector<std::string> kvNames{"key", "value"};
        std::vector<std::unique_ptr<ColumnWriter>> childrenWriters;
        childrenWriters.reserve(2);
        for (auto i = 0u; i < 2; i++) {
            auto childWriter = createWriterRecursive(schemas, writer, kvTypes[i], kvNames[i],
                schemaPathToCreate, mm, maxRepeatToCreate + 1, maxDefineToCreate + 2, i != 0);
            childrenWriters.push_back(std::move(childWriter));
        }
        auto structWriter = std::make_unique<StructColumnWriter>(writer, schemaIdx,
            schemaPathToCreate, maxRepeatToCreate, maxDefineToCreate, std::move(childrenWriters),
            canHaveNullsToCreate);
        return std::make_unique<VarListColumnWriter>(writer, schemaIdx, schemaPathToCreate,
            maxRepeatToCreate, maxDefineToCreate, std::move(structWriter), canHaveNullsToCreate);
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
        case LogicalTypeID::INT8:
            return std::make_unique<StandardColumnWriter<int8_t, int32_t>>(writer, schemaIdx,
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
        case LogicalTypeID::TIMESTAMP_TZ:
        case LogicalTypeID::TIMESTAMP_MS:
        case LogicalTypeID::TIMESTAMP:
        case LogicalTypeID::INT64:
            return std::make_unique<StandardColumnWriter<int64_t, int64_t>>(writer, schemaIdx,
                std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
                canHaveNullsToCreate);
        case LogicalTypeID::TIMESTAMP_NS:
            return make_unique<StandardColumnWriter<int64_t, int64_t, ParquetTimestampNSOperator>>(
                writer, schemaIdx, std::move(schemaPathToCreate), maxRepeatToCreate,
                maxDefineToCreate, canHaveNullsToCreate);
        case LogicalTypeID::TIMESTAMP_SEC:
            return make_unique<StandardColumnWriter<int64_t, int64_t, ParquetTimestampSOperator>>(
                writer, schemaIdx, std::move(schemaPathToCreate), maxRepeatToCreate,
                maxDefineToCreate, canHaveNullsToCreate);
        case LogicalTypeID::INT128:
            return std::make_unique<StandardColumnWriter<int128_t, double, ParquetInt128Operator>>(
                writer, schemaIdx, std::move(schemaPathToCreate), maxRepeatToCreate,
                maxDefineToCreate, canHaveNullsToCreate);
        case LogicalTypeID::UINT8:
            return std::make_unique<StandardColumnWriter<uint8_t, int32_t>>(writer, schemaIdx,
                std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
                canHaveNullsToCreate);
        case LogicalTypeID::UINT16:
            return std::make_unique<StandardColumnWriter<uint16_t, int32_t>>(writer, schemaIdx,
                std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
                canHaveNullsToCreate);
        case LogicalTypeID::UINT32:
            return std::make_unique<StandardColumnWriter<uint32_t, uint32_t>>(writer, schemaIdx,
                std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
                canHaveNullsToCreate);
        case LogicalTypeID::UINT64:
            return std::make_unique<StandardColumnWriter<uint64_t, uint64_t>>(writer, schemaIdx,
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
        case LogicalTypeID::INTERVAL:
            return std::make_unique<IntervalColumnWriter>(writer, schemaIdx,
                std::move(schemaPathToCreate), maxRepeatToCreate, maxDefineToCreate,
                canHaveNullsToCreate);
        default:
            KU_UNREACHABLE;
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
        KU_ASSERT(compressedSize <= kuzu_snappy::MaxCompressedLength(bufferedSerializer.getSize()));
    } break;
    default:
        KU_UNREACHABLE;
    }

    if (compressedSize > uint64_t(function::NumericLimits<int32_t>::maximum())) {
        throw RuntimeException(
            stringFormat("Parquet writer: {} compressed page size out of range for type integer",
                bufferedSerializer.getSize()));
    }
}

} // namespace processor
} // namespace kuzu
