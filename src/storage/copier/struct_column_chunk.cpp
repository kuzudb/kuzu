#include "storage/copier/struct_column_chunk.h"

#include "common/string_utils.h"
#include "storage/copier/string_column_chunk.h"
#include "storage/copier/var_list_column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

StructColumnChunk::StructColumnChunk(LogicalType dataType, CopyDescription* copyDescription)
    : ColumnChunk{std::move(dataType), copyDescription} {
    auto fieldTypes = StructType::getFieldTypes(&this->dataType);
    childrenChunks.resize(fieldTypes.size());
    for (auto i = 0u; i < fieldTypes.size(); i++) {
        childrenChunks[i] = ColumnChunkFactory::createColumnChunk(*fieldTypes[i], copyDescription);
    }
}

void StructColumnChunk::append(
    arrow::Array* array, common::offset_t startPosInChunk, uint32_t numValuesToAppend) {
    switch (array->type_id()) {
    case arrow::Type::STRUCT: {
        auto structArray = (arrow::StructArray*)array;
        auto arrayData = structArray->data();
        if (common::StructType::getNumFields(&dataType) != structArray->type()->fields().size()) {
            throw CopyException{"Unmatched number of struct fields in StructColumnChunk::append."};
        }
        for (auto i = 0u; i < structArray->num_fields(); i++) {
            auto fieldName = structArray->type()->fields()[i]->name();
            auto fieldIdx = common::StructType::getFieldIdx(&dataType, fieldName);
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
    } break;
    case arrow::Type::STRING: {
        auto* stringArray = (arrow::StringArray*)array;
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
    } break;
    default: {
        throw NotImplementedException("StructColumnChunk::append");
    }
    }
}

void StructColumnChunk::append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
    common::offset_t startPosInChunk, uint32_t numValuesToAppend) {
    auto otherStructChunk = dynamic_cast<StructColumnChunk*>(other);
    assert(other->getNumChildren() == getNumChildren());
    nullChunk->append(
        other->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    for (auto i = 0u; i < getNumChildren(); i++) {
        childrenChunks[i]->append(otherStructChunk->childrenChunks[i].get(), startPosInOtherChunk,
            startPosInChunk, numValuesToAppend);
    }
}

void StructColumnChunk::setStructFields(const char* value, uint64_t length, uint64_t pos) {
    // Removes the leading and trailing '{', '}';
    auto structString = std::string(value, length).substr(1, length - 2);
    auto structFieldIdxAndValuePairs = parseStructFieldNameAndValues(dataType, structString);
    for (auto& fieldIdxAndValue : structFieldIdxAndValuePairs) {
        setValueToStructField(pos, fieldIdxAndValue.fieldValue, fieldIdxAndValue.fieldIdx);
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

std::vector<StructFieldIdxAndValue> StructColumnChunk::parseStructFieldNameAndValues(
    LogicalType& type, const std::string& structString) {
    std::vector<StructFieldIdxAndValue> structFieldIdxAndValueParis;
    uint64_t curPos = 0u;
    while (curPos < structString.length()) {
        auto fieldName = parseStructFieldName(structString, curPos);
        auto fieldIdx = StructType::getFieldIdx(&type, fieldName);
        if (fieldIdx == INVALID_STRUCT_FIELD_IDX) {
            throw ParserException{"Invalid struct field name: " + fieldName};
        }
        auto structFieldValue = parseStructFieldValue(structString, curPos);
        structFieldIdxAndValueParis.emplace_back(fieldIdx, structFieldValue);
    }
    return structFieldIdxAndValueParis;
}

std::string StructColumnChunk::parseStructFieldName(
    const std::string& structString, uint64_t& curPos) {
    auto startPos = curPos;
    while (curPos < structString.length()) {
        if (structString[curPos] == ':') {
            auto structFieldName = structString.substr(startPos, curPos - startPos);
            StringUtils::removeWhiteSpaces(structFieldName);
            curPos++;
            return structFieldName;
        }
        curPos++;
    }
    throw ParserException{"Invalid struct string: " + structString};
}

std::string StructColumnChunk::parseStructFieldValue(
    const std::string& structString, uint64_t& curPos) {
    auto numListBeginChars = 0u;
    auto numStructBeginChars = 0u;
    auto numDoubleQuotes = 0u;
    auto numSingleQuotes = 0u;
    // Skip leading white spaces.
    while (structString[curPos] == ' ') {
        curPos++;
    }
    auto startPos = curPos;
    while (curPos < structString.length()) {
        auto curChar = structString[curPos];
        if (curChar == '{') {
            numStructBeginChars++;
        } else if (curChar == '}') {
            numStructBeginChars--;
        } else if (curChar == copyDescription->csvReaderConfig->listBeginChar) {
            numListBeginChars++;
        } else if (curChar == copyDescription->csvReaderConfig->listEndChar) {
            numListBeginChars--;
        } else if (curChar == '"') {
            numDoubleQuotes ^= 1;
        } else if (curChar == '\'') {
            numSingleQuotes ^= 1;
        } else if (curChar == ',') {
            if (numListBeginChars == 0 && numStructBeginChars == 0 && numDoubleQuotes == 0 &&
                numSingleQuotes == 0) {
                curPos++;
                return structString.substr(startPos, curPos - startPos - 1);
            }
        }
        curPos++;
    }
    if (numListBeginChars == 0 && numStructBeginChars == 0 && numDoubleQuotes == 0 &&
        numSingleQuotes == 0) {
        return structString.substr(startPos, curPos - startPos);
    } else {
        throw common::ParserException{"Invalid struct string: " + structString};
    }
}

void StructColumnChunk::write(const common::Value& val, uint64_t posToWrite) {
    assert(val.getDataType()->getPhysicalType() == PhysicalTypeID::STRUCT);
    auto numElements = NestedVal::getChildrenSize(&val);
    for (auto i = 0u; i < numElements; i++) {
        childrenChunks[i]->write(*NestedVal::getChildVal(&val, i), posToWrite);
    }
}

} // namespace storage
} // namespace kuzu
