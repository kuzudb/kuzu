#include "processor/operator/persistent/parquet_column_writer.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

void ParquetColumnWriter::nextParquetColumn(LogicalTypeID logicalTypeID) {
    columnWriter = rowGroupWriter->column(currentParquetColumn);
    currentParquetColumn++;
    if (currentParquetColumn == totalColumns) {
        currentParquetColumn = 0;
    }
}

void ParquetColumnWriter::writePrimitiveValue(
    LogicalTypeID logicalTypeID, uint8_t* value, int16_t definitionLevel, int16_t repetitionLevel) {
    switch (logicalTypeID) {
    case LogicalTypeID::BOOL: {
        auto val = *reinterpret_cast<bool*>(value);
        auto boolWriter = static_cast<parquet::BoolWriter*>(columnWriter);
        boolWriter->WriteBatch(1, &definitionLevel, &repetitionLevel, &val);
    } break;
    case LogicalTypeID::INT16: {
        auto val = static_cast<int32_t>(*reinterpret_cast<int16_t*>(value));
        auto int32Writer = static_cast<parquet::Int32Writer*>(columnWriter);
        int32Writer->WriteBatch(1, &definitionLevel, &repetitionLevel, &val);
    } break;
    case LogicalTypeID::INT32: {
        auto val = *reinterpret_cast<int32_t*>(value);
        auto int32Writer = static_cast<parquet::Int32Writer*>(columnWriter);
        int32Writer->WriteBatch(1, &definitionLevel, &repetitionLevel, &val);
    } break;
    case LogicalTypeID::INT64: {
        auto val = *reinterpret_cast<int64_t*>(value);
        auto int64Writer = static_cast<parquet::Int64Writer*>(columnWriter);
        int64Writer->WriteBatch(1, &definitionLevel, &repetitionLevel, &val);
    } break;
    case LogicalTypeID::DOUBLE: {
        auto val = *reinterpret_cast<double*>(value);
        auto doubleWriter = static_cast<parquet::DoubleWriter*>(columnWriter);
        doubleWriter->WriteBatch(1, &definitionLevel, &repetitionLevel, &val);
    } break;
    case LogicalTypeID::FLOAT: {
        auto val = *reinterpret_cast<float*>(value);
        auto floatWriter = static_cast<parquet::FloatWriter*>(columnWriter);
        floatWriter->WriteBatch(1, &definitionLevel, &repetitionLevel, &val);
    } break;
    case LogicalTypeID::DATE: {
        auto val = *reinterpret_cast<date_t*>(value);
        auto int32Writer = static_cast<parquet::Int32Writer*>(columnWriter);
        int32Writer->WriteBatch(1, &definitionLevel, &repetitionLevel, &val.days);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        auto val = *reinterpret_cast<timestamp_t*>(value);
        auto int64Writer = static_cast<parquet::Int64Writer*>(columnWriter);
        int64Writer->WriteBatch(1, &definitionLevel, &repetitionLevel, &val.value);
    } break;
    case LogicalTypeID::INTERVAL: {
        auto val = *reinterpret_cast<interval_t*>(value);
        auto fixedLenByteArrayWriter = static_cast<parquet::FixedLenByteArrayWriter*>(columnWriter);
        uint32_t milliseconds = val.micros / Interval::MICROS_PER_MSEC;
        uint8_t interval[12];
        writeLittleEndianUint32(interval, val.months);
        writeLittleEndianUint32(interval + 4, val.days);
        writeLittleEndianUint32(interval + 8, milliseconds);
        parquet::FixedLenByteArray intervalByteArray;
        intervalByteArray.ptr = interval;
        fixedLenByteArrayWriter->WriteBatch(1, nullptr, nullptr, &intervalByteArray);
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        auto val = *reinterpret_cast<internalID_t*>(value);
        auto byteArrayWriter = static_cast<parquet::ByteArrayWriter*>(columnWriter);
        auto internalID = std::to_string(val.tableID) + ":" + std::to_string(val.offset);
        parquet::ByteArray valueToWrite;
        valueToWrite.ptr = reinterpret_cast<const uint8_t*>(&internalID[0]);
        valueToWrite.len = internalID.length();
        byteArrayWriter->WriteBatch(1, &definitionLevel, &repetitionLevel, &valueToWrite);
    } break;
    case LogicalTypeID::STRING: {
        auto val = *reinterpret_cast<ku_string_t*>(value);
        auto byteArrayWriter = static_cast<parquet::ByteArrayWriter*>(columnWriter);
        parquet::ByteArray valueToWrite;
        valueToWrite.ptr = &val.getData()[0];
        valueToWrite.len = val.len;
        byteArrayWriter->WriteBatch(1, &definitionLevel, &repetitionLevel, &valueToWrite);
    } break;
    default:
        throw NotImplementedException("ParquetWriter::writePrimitiveValue");
    }
}

void ParquetColumnWriter::writeColumn(
    int column, ValueVector* vector, parquet::RowGroupWriter* writer) {
    initNewColumn();
    currentColumn = column;
    rowGroupWriter = writer;
    auto selPos = vector->state->selVector->selectedPositions[0];
    uint8_t* value = vector->getData() + vector->getNumBytesPerValue() * selPos;
    estimatedRowBytes += vector->getNumBytesPerValue();
    if (LogicalTypeUtils::isNested(vector->dataType)) {
        std::map<std::string, ParquetColumn> batch;
        extractNested(value, vector, batch);
        // write primitives after extraction
        for (auto& [_, column] : batch) {
            nextParquetColumn(column.logicalTypeID);
            for (auto j = 0; j < column.values.size(); ++j) {
                writePrimitiveValue(column.logicalTypeID, column.values[j],
                    column.definitionLevels[j], column.repetitionLevels[j]);
            }
        }
    } else {
        nextParquetColumn(vector->dataType.getLogicalTypeID());
        writePrimitiveValue(vector->dataType.getLogicalTypeID(), value);
    }
}

int ParquetColumnWriter::getRepetitionLevel(
    int currentElementIdx, int parentElementIdx, int depth) {
    if (isListStarting)
        return 0;
    if (currentElementIdx == 0) {
        if (parentElementIdx == 0) {
            int result = depth - 2; // depth - parent depth - 1
            return (result < 0) ? 0 : result;
        }
        return depth - 1;
    }
    return depth;
}

//   For structs, parquet flattens the data so each value has its own column if
//   the key is the same. Examples:
//
//   {a: {b: 1, c: 2}, d: {e: 3, f: 4}}
//   numberOfPrimitiveNodes = 4
//   column[0] = {1};
//   column[1] = {2};
//   column[2] = {3};
//   column[3] = {4};
//
//   In this case, the last 'b' is a new column since it is nested in 'c'
//   {a: [{b: 1}, {b: 2}], c: [{b: 3}]}
//   numberOfPrimitiveNodes = 3
//   column[0] = {1,2};
//   column[1] = {3};
//
//   [{a:1, b:2}, {a:3, b:4}]
//   numberOfPrimitiveNodes = 2
//   column[0] = {1,3};
//   column[1] = {2,4};
void ParquetColumnWriter::addToParquetColumns(uint8_t* value, ValueVector* vector,
    std::map<std::string, ParquetColumn>& parquetColumns, int currentElementIdx,
    int parentElementIdx, int depth, std::string parentStructFieldName) {
    int repetitionLevel = getRepetitionLevel(currentElementIdx, parentElementIdx, depth);
    if (LogicalTypeUtils::isNested(vector->dataType)) {
        extractNested(value, vector, parquetColumns, currentElementIdx, parentElementIdx, depth,
            parentStructFieldName);
    } else {
        // We define parquetBatch[column] to store the necessary information to
        // write in each parquet column using writeBatch. One Kuzu column can
        // have multiple parquet columns.
        parquetColumns[parentStructFieldName].repetitionLevels.push_back(repetitionLevel);
        parquetColumns[parentStructFieldName].definitionLevels.push_back(
            maxDefinitionLevels[currentColumn]);
        parquetColumns[parentStructFieldName].values.push_back(value);
        parquetColumns[parentStructFieldName].logicalTypeID = vector->dataType.getLogicalTypeID();
    }
}

void ParquetColumnWriter::extractNested(uint8_t* value, const ValueVector* vector,
    std::map<std::string, ParquetColumn>& parquetColumns, int currentElementIdx,
    int parentElementIdx, int depth, std::string parentStructFieldName) {
    switch (vector->dataType.getLogicalTypeID()) {
    case LogicalTypeID::VAR_LIST:
        return extractList(*reinterpret_cast<list_entry_t*>(value), vector, parquetColumns,
            currentElementIdx, parentElementIdx, depth, parentStructFieldName);
    case LogicalTypeID::STRUCT:
        return extractStruct(*reinterpret_cast<struct_entry_t*>(value), vector, parquetColumns,
            currentElementIdx, parentElementIdx, depth, parentStructFieldName);
    default:
        throw NotImplementedException("ParquetColumnWriter::extractNested");
    }
}

void ParquetColumnWriter::extractList(const list_entry_t& list, const ValueVector* vector,
    std::map<std::string, ParquetColumn>& parquetColumns, int currentElementIdx,
    int parentElementIdx, int depth, std::string parentStructFieldName) {
    auto values = ListVector::getListValues(vector, list);
    auto dataVector = ListVector::getDataVector(vector);
    depth++;
    int elementsToAdd = (list.size == 0) ? 1 : list.size;
    for (auto i = 0u; i < elementsToAdd; ++i) {
        // this is used to always set the repetition level to 0 when a kuzu
        // column starts
        isListStarting = isListStarting && i == 0;
        addToParquetColumns(values, dataVector, parquetColumns, i /* currentElementIdx */,
            currentElementIdx /* parentElementIdx */, depth, parentStructFieldName);
        values += ListVector::getDataVector(vector)->getNumBytesPerValue();
    }
}

void ParquetColumnWriter::extractStruct(const struct_entry_t& val, const ValueVector* vector,
    std::map<std::string, ParquetColumn>& parquetColumns, int currentElementIdx,
    int parentElementIdx, int depth, std::string parentStructFieldName) {
    auto fields = StructType::getFields(&vector->dataType);
    auto structNames = StructType::getFieldNames(&vector->dataType);
    for (auto i = 0u; i < fields.size(); ++i) {
        auto fieldVector = StructVector::getFieldVector(vector, i);
        std::string fieldName = parentStructFieldName + structNames[i];
        addToParquetColumns(fieldVector->getData() + fieldVector->getNumBytesPerValue() * val.pos,
            fieldVector.get(), parquetColumns, currentElementIdx, parentElementIdx, depth,
            fieldName);
    }
}

void ParquetColumnWriter::writeLittleEndianUint32(uint8_t* buffer, uint32_t value) {
    buffer[0] = (value & 0x000000FF);
    buffer[1] = (value & 0x0000FF00) >> 8;
    buffer[2] = (value & 0x00FF0000) >> 16;
    buffer[3] = (value & 0xFF000000) >> 24;
}

} // namespace processor
} // namespace kuzu
