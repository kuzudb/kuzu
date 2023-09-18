#include "storage/store/struct_column_chunk.h"

#include "common/exception/copy.h"
#include "common/exception/not_implemented.h"
#include "common/exception/parser.h"
#include "common/string_utils.h"
#include "common/types/value/nested.h"
#include "storage/store/string_column_chunk.h"
#include "storage/store/table_copy_utils.h"
#include "storage/store/var_list_column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

StructColumnChunk::StructColumnChunk(
    LogicalType dataType, std::unique_ptr<common::CSVReaderConfig> csvReaderConfig)
    : ColumnChunk{std::move(dataType), std::move(csvReaderConfig)} {
    auto fieldTypes = StructType::getFieldTypes(&this->dataType);
    childrenChunks.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        childrenChunks[i] =
            ColumnChunkFactory::createColumnChunk(*fieldTypes[i], this->csvReaderConfig.get());
    }
}

void StructColumnChunk::append(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    switch (array->type_id()) {
    case arrow::Type::STRUCT: {
        copyStructFromArrowStruct(array, startPosInChunk, numValuesToAppend);
    } break;
    case arrow::Type::STRING: {
        copyStructFromArrowString<arrow::StringArray>(array, startPosInChunk, numValuesToAppend);

    } break;
    case arrow::Type::LARGE_STRING: {
        copyStructFromArrowString<arrow::LargeStringArray>(
            array, startPosInChunk, numValuesToAppend);
    } break;
    default: {
        throw NotImplementedException("StructColumnChunk::append");
    }
    }
    numValues += numValuesToAppend;
}

void StructColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto otherStructChunk = dynamic_cast<StructColumnChunk*>(other);
    assert(other->getNumChildren() == getNumChildren());
    nullChunk->append(
        other->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    for (auto i = 0u; i < getNumChildren(); i++) {
        childrenChunks[i]->append(otherStructChunk->childrenChunks[i].get(), startPosInOtherChunk,
            startPosInChunk, numValuesToAppend);
    }
    numValues += numValuesToAppend;
}

void StructColumnChunk::append(common::ValueVector* vector, common::offset_t startPosInChunk) {
    auto numFields = StructType::getNumFields(&dataType);
    for (auto i = 0u; i < numFields; i++) {
        childrenChunks[i]->append(StructVector::getFieldVector(vector, i).get(), startPosInChunk);
    }
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        nullChunk->setNull(
            startPosInChunk + i, vector->isNull(vector->state->selVector->selectedPositions[i]));
    }
    numValues += vector->state->selVector->selectedSize;
}

void StructColumnChunk::setStructFields(const char* value, uint64_t length, uint64_t pos) {
    // Removes the leading and the trailing brackets.
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::STRUCT: {
        auto structString = std::string(value, length).substr(1, length - 2);
        auto structFieldIdxAndValuePairs =
            TableCopyUtils::parseStructFieldNameAndValues(dataType, structString, *csvReaderConfig);
        for (auto& fieldIdxAndValue : structFieldIdxAndValuePairs) {
            setValueToStructField(pos, fieldIdxAndValue.fieldValue, fieldIdxAndValue.fieldIdx);
        }
    } break;
    case LogicalTypeID::UNION: {
        union_field_idx_t selectedFieldIdx = INVALID_STRUCT_FIELD_IDX;
        for (auto i = 0u; i < UnionType::getNumFields(&dataType); i++) {
            auto internalFieldIdx = UnionType::getInternalFieldIdx(i);
            if (TableCopyUtils::tryCast(*UnionType::getFieldType(&dataType, i), value, length)) {
                childrenChunks[internalFieldIdx]->getNullChunk()->setNull(pos, false /* isNull */);
                setValueToStructField(pos, std::string(value, length), internalFieldIdx);
                selectedFieldIdx = i;
                break;
            } else {
                childrenChunks[internalFieldIdx]->getNullChunk()->setNull(pos, true /* isNull */);
            }
        }
        if (selectedFieldIdx == INVALID_STRUCT_FIELD_IDX) {
            throw ParserException{StringUtils::string_format(
                "No parsing rule matches value: {}.", std::string(value, length))};
        }
        childrenChunks[UnionType::TAG_FIELD_IDX]->setValue(selectedFieldIdx, pos);
        childrenChunks[UnionType::TAG_FIELD_IDX]->getNullChunk()->setNull(pos, false /* isNull */);
    } break;
    default: {
        throw NotImplementedException("StructColumnChunk::setStructFields");
    }
    }
}

void StructColumnChunk::setValueToStructField(
    offset_t pos, const std::string& structFieldValue, struct_field_idx_t structFiledIdx) {
    auto fieldChunk = childrenChunks[structFiledIdx].get();
    switch (fieldChunk->getDataType().getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        fieldChunk->setValueFromString<int64_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::INT32: {
        fieldChunk->setValueFromString<int32_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::INT16: {
        fieldChunk->setValueFromString<int16_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::INT8: {
        fieldChunk->setValueFromString<int8_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::UINT64: {
        fieldChunk->setValueFromString<uint64_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::UINT32: {
        fieldChunk->setValueFromString<uint32_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::UINT16: {
        fieldChunk->setValueFromString<uint16_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::UINT8: {
        fieldChunk->setValueFromString<uint8_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::DOUBLE: {
        fieldChunk->setValueFromString<double_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::FLOAT: {
        fieldChunk->setValueFromString<float_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::BOOL: {
        fieldChunk->setValueFromString<bool>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::DATE: {
        fieldChunk->setValueFromString<date_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        fieldChunk->setValueFromString<timestamp_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::INTERVAL: {
        fieldChunk->setValueFromString<interval_t>(
            structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::STRING: {
        reinterpret_cast<StringColumnChunk*>(fieldChunk)
            ->setValueFromString<ku_string_t>(
                structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::VAR_LIST: {
        reinterpret_cast<VarListColumnChunk*>(fieldChunk)
            ->setValueFromString(structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    case LogicalTypeID::STRUCT: {
        reinterpret_cast<StructColumnChunk*>(fieldChunk)
            ->setStructFields(structFieldValue.c_str(), structFieldValue.length(), pos);
    } break;
    default: {
        throw NotImplementedException{StringUtils::string_format(
            "Unsupported data type: {}.", LogicalTypeUtils::dataTypeToString(dataType))};
    }
    }
}

void StructColumnChunk::write(const Value& val, uint64_t posToWrite) {
    assert(val.getDataType()->getPhysicalType() == PhysicalTypeID::STRUCT);
    auto numElements = NestedVal::getChildrenSize(&val);
    for (auto i = 0u; i < numElements; i++) {
        childrenChunks[i]->write(*NestedVal::getChildVal(&val, i), posToWrite);
    }
}

void StructColumnChunk::copyStructFromArrowStruct(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto structArray = (arrow::StructArray*)array;
    auto arrayData = structArray->data();
    if (StructType::getNumFields(&dataType) != structArray->type()->fields().size()) {
        throw CopyException{"Unmatched number of struct fields in StructColumnChunk::append."};
    }
    for (auto i = 0u; i < structArray->num_fields(); i++) {
        auto fieldName = structArray->type()->fields()[i]->name();
        auto fieldIdx = StructType::getFieldIdx(&dataType, fieldName);
        if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
            throw CopyException{"Unmatched struct field name: " + fieldName + "."};
        }
        childrenChunks[fieldIdx]->append(
            structArray->field(i).get(), startPosInChunk, numValuesToAppend);
    }
    if (arrayData->MayHaveNulls()) {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            if (arrayData->IsNull(i)) {
                nullChunk->setNull(posInChunk, true);
                continue;
            }
        }
    }
}

template<typename ARROW_TYPE>
void StructColumnChunk::copyStructFromArrowString(
    arrow::Array* array, offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto* stringArray = (ARROW_TYPE*)array;
    auto arrayData = stringArray->data();
    if (arrayData->MayHaveNulls()) {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            if (arrayData->IsNull(i)) {
                nullChunk->setNull(posInChunk, true);
                continue;
            }
            auto value = stringArray->GetView(i);
            setStructFields(value.data(), value.length(), posInChunk);
        }
    } else {
        for (auto i = 0u; i < numValuesToAppend; i++) {
            auto posInChunk = startPosInChunk + i;
            auto value = stringArray->GetView(i);
            setStructFields(value.data(), value.length(), posInChunk);
        }
    }
}

} // namespace storage
} // namespace kuzu
