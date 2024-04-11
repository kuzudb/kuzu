#include "common/arrow/arrow_row_batch.h"

#include <cstring>

#include "common/exception/runtime.h"
#include "common/types/uuid.h"
#include "common/types/value/node.h"
#include "common/types/value/rel.h"
#include "common/types/value/value.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace common {

ArrowRowBatch::ArrowRowBatch(std::vector<LogicalType> types, std::int64_t capacity)
    : types{std::move(types)}, numTuples{0} {
    auto numVectors = this->types.size();
    vectors.resize(numVectors);
    for (auto i = 0u; i < numVectors; i++) {
        vectors[i] = createVector(this->types[i], capacity);
    }
}

// TODO(Ziyi): use physical type instead of logical type here.
template<LogicalTypeID DT>
void ArrowRowBatch::templateInitializeVector(ArrowVector* vector, const LogicalType& /*type*/,
    std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    vector->data.reserve(storage::StorageUtils::getDataTypeSize(LogicalType{DT}) * capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::BOOL>(ArrowVector* vector,
    const LogicalType& /*type*/, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    vector->data.reserve(getNumBytesForBits(capacity));
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::STRING>(ArrowVector* vector,
    const LogicalType& /*type*/, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    // Initialize offsets and string values buffer.
    vector->data.reserve((capacity + 1) * sizeof(std::uint32_t));
    ((std::uint32_t*)vector->data.data())[0] = 0;
    vector->overflow.reserve(capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::LIST>(ArrowVector* vector,
    const LogicalType& type, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    auto childType = *ListType::getChildType(&type);
    // Initialize offsets and child buffer.
    vector->data.reserve((capacity + 1) * sizeof(std::uint32_t));
    ((std::uint32_t*)vector->data.data())[0] = 0;
    auto childVector = createVector(childType, capacity);
    vector->childData.push_back(std::move(childVector));
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::ARRAY>(ArrowVector* vector,
    const LogicalType& type, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    auto childType = *ArrayType::getChildType(&type);
    // Initialize offsets and child buffer.
    vector->data.reserve((capacity + 1) * sizeof(std::uint32_t));
    ((std::uint32_t*)vector->data.data())[0] = 0;
    auto childVector = createVector(childType, capacity);
    vector->childData.push_back(std::move(childVector));
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::STRUCT>(ArrowVector* vector,
    const LogicalType& type, std::int64_t capacity) {
    initializeStructVector(vector, type, capacity);
}

void ArrowRowBatch::initializeStructVector(ArrowVector* vector, const LogicalType& type,
    std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    for (auto& childType : StructType::getFieldTypes(&type)) {
        auto childVector = createVector(*childType, capacity);
        vector->childData.push_back(std::move(childVector));
    }
}

void ArrowRowBatch::initializeInternalIDVector(ArrowVector* vector, const LogicalType& /*type*/,
    std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    vector->childData.resize(2);
    auto childVector1 = createVector(*LogicalType::INT64(), capacity);
    vector->childData[0] = std::move(childVector1);
    auto childVector2 = createVector(*LogicalType::INT64(), capacity);
    vector->childData[1] = std::move(childVector2);
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::INTERNAL_ID>(ArrowVector* vector,
    const LogicalType& type, std::int64_t capacity) {
    initializeInternalIDVector(vector, type, capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::NODE>(ArrowVector* vector,
    const LogicalType& type, std::int64_t capacity) {
    initializeStructVector(vector, type, capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::REL>(ArrowVector* vector,
    const LogicalType& type, std::int64_t capacity) {
    initializeStructVector(vector, type, capacity);
}

std::unique_ptr<ArrowVector> ArrowRowBatch::createVector(const LogicalType& type,
    std::int64_t capacity) {
    auto result = std::make_unique<ArrowVector>();
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        templateInitializeVector<LogicalTypeID::BOOL>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::INT128: {
        templateInitializeVector<LogicalTypeID::INT128>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::INT64: {
        templateInitializeVector<LogicalTypeID::INT64>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::INT32: {
        templateInitializeVector<LogicalTypeID::INT32>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::INT16: {
        templateInitializeVector<LogicalTypeID::INT16>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::INT8: {
        templateInitializeVector<LogicalTypeID::INT8>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::UINT64: {
        templateInitializeVector<LogicalTypeID::UINT64>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::UINT32: {
        templateInitializeVector<LogicalTypeID::UINT32>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::UINT16: {
        templateInitializeVector<LogicalTypeID::UINT16>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::UINT8: {
        templateInitializeVector<LogicalTypeID::UINT8>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::DOUBLE: {
        templateInitializeVector<LogicalTypeID::DOUBLE>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::FLOAT: {
        templateInitializeVector<LogicalTypeID::FLOAT>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::DATE: {
        templateInitializeVector<LogicalTypeID::DATE>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        templateInitializeVector<LogicalTypeID::TIMESTAMP_MS>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        templateInitializeVector<LogicalTypeID::TIMESTAMP_NS>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        templateInitializeVector<LogicalTypeID::TIMESTAMP_SEC>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::TIMESTAMP_TZ: {
        templateInitializeVector<LogicalTypeID::TIMESTAMP_TZ>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        templateInitializeVector<LogicalTypeID::TIMESTAMP>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::INTERVAL: {
        templateInitializeVector<LogicalTypeID::INTERVAL>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::UUID:
    case LogicalTypeID::STRING: {
        templateInitializeVector<LogicalTypeID::STRING>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::LIST: {
        templateInitializeVector<LogicalTypeID::LIST>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::ARRAY: {
        templateInitializeVector<LogicalTypeID::ARRAY>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::STRUCT: {
        templateInitializeVector<LogicalTypeID::STRUCT>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        templateInitializeVector<LogicalTypeID::INTERNAL_ID>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::NODE: {
        templateInitializeVector<LogicalTypeID::NODE>(result.get(), type, capacity);
    } break;
    case LogicalTypeID::REL: {
        templateInitializeVector<LogicalTypeID::REL>(result.get(), type, capacity);
    } break;
    default: {
        // LCOV_EXCL_START
        throw common::RuntimeException{
            common::stringFormat("Unsupported type: {} for arrow conversion.", type.toString())};
        // LCOV_EXCL_STOP
    }
    }
    return result;
}

static void getBitPosition(std::int64_t pos, std::int64_t& bytePos, std::int64_t& bitOffset) {
    bytePos = pos >> 3;
    bitOffset = pos - (bytePos << 3);
}

static void setBitToZero(std::uint8_t* data, std::int64_t pos) {
    std::int64_t bytePos, bitOffset;
    getBitPosition(pos, bytePos, bitOffset);
    data[bytePos] &= ~((std::uint64_t)1 << bitOffset);
}

static void setBitToOne(std::uint8_t* data, std::int64_t pos) {
    std::int64_t bytePos, bitOffset;
    getBitPosition(pos, bytePos, bitOffset);
    data[bytePos] |= ((std::uint64_t)1 << bitOffset);
}

void ArrowRowBatch::appendValue(ArrowVector* vector, const LogicalType& type, Value* value) {
    if (value->isNull()) {
        copyNullValue(vector, value, vector->numValues);
    } else {
        copyNonNullValue(vector, type, value, vector->numValues);
    }
    vector->numValues++;
}

template<LogicalTypeID DT>
void ArrowRowBatch::templateCopyNonNullValue(ArrowVector* vector, const LogicalType& /*type*/,
    Value* value, std::int64_t pos) {
    auto valSize = storage::StorageUtils::getDataTypeSize(LogicalType{DT});
    std::memcpy(vector->data.data() + pos * valSize, &value->val, valSize);
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::BOOL>(ArrowVector* vector,
    const LogicalType& /*type*/, Value* value, std::int64_t pos) {
    if (value->val.booleanVal) {
        setBitToOne(vector->data.data(), pos);
    } else {
        setBitToZero(vector->data.data(), pos);
    }
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::STRING>(ArrowVector* vector,
    const LogicalType& /*type*/, Value* value, std::int64_t pos) {
    auto offsets = (std::uint32_t*)vector->data.data();
    auto strLength = value->strVal.length();
    offsets[pos + 1] = offsets[pos] + strLength;
    vector->overflow.resize(offsets[pos + 1]);
    std::memcpy(vector->overflow.data() + offsets[pos], value->strVal.data(), strLength);
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::UUID>(ArrowVector* vector,
    const LogicalType& /*type*/, Value* value, std::int64_t pos) {
    auto offsets = (std::uint32_t*)vector->data.data();
    auto str = UUID::toString(value->val.int128Val);
    auto strLength = str.length();
    offsets[pos + 1] = offsets[pos] + strLength;
    vector->overflow.resize(offsets[pos + 1]);
    std::memcpy(vector->overflow.data() + offsets[pos], str.data(), strLength);
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::LIST>(ArrowVector* vector,
    const LogicalType& type, Value* value, std::int64_t pos) {
    vector->data.resize((pos + 2) * sizeof(std::uint32_t));
    auto offsets = (std::uint32_t*)vector->data.data();
    auto numElements = value->childrenSize;
    offsets[pos + 1] = offsets[pos] + numElements;
    auto numChildElements = offsets[pos + 1] + 1;
    auto currentNumBytesForChildValidity = vector->childData[0]->validity.size();
    auto numBytesForChildValidity = getNumBytesForBits(numChildElements);
    vector->childData[0]->validity.resize(numBytesForChildValidity);
    // Initialize validity mask which is used to mark each value is valid (non-null) or not (null).
    for (auto i = currentNumBytesForChildValidity; i < numBytesForChildValidity; i++) {
        vector->childData[0]->validity.data()[i] = 0xFF; // Init each value to be valid (as 1).
    }
    // If vector->childData[0] is a LIST, its data buffer will be resized when we add a new
    // value into it
    // If vector->childData[0] is an ARRAY, its data buffer is supposed to be empty,
    // so we don't resize it here
    if (ListType::getChildType(&type)->getLogicalTypeID() != LogicalTypeID::LIST &&
        ListType::getChildType(&type)->getLogicalTypeID() != LogicalTypeID::ARRAY) {
        vector->childData[0]->data.resize(numChildElements * storage::StorageUtils::getDataTypeSize(
                                                                 *ListType::getChildType(&type)));
    }
    for (auto i = 0u; i < numElements; i++) {
        appendValue(vector->childData[0].get(), *ListType::getChildType(&type),
            value->children[i].get());
    }
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::ARRAY>(ArrowVector* vector,
    const LogicalType& type, Value* value, std::int64_t pos) {
    vector->data.resize((pos + 2) * sizeof(std::uint32_t));
    auto offsets = (std::uint32_t*)vector->data.data();
    auto numElements = value->childrenSize;
    offsets[pos + 1] = offsets[pos] + numElements;
    auto numChildElements = offsets[pos + 1] + 1;
    auto currentNumBytesForChildValidity = vector->childData[0]->validity.size();
    auto numBytesForChildValidity = getNumBytesForBits(numChildElements);
    vector->childData[0]->validity.resize(numBytesForChildValidity);
    // Initialize validity mask which is used to mark each value is valid (non-null) or not (null).
    for (auto i = currentNumBytesForChildValidity; i < numBytesForChildValidity; i++) {
        vector->childData[0]->validity.data()[i] = 0xFF; // Init each value to be valid (as 1).
    }
    // If vector->childData[0] is a LIST, its data buffer will be resized when we add a new
    // value into it
    // If vector->childData[0] is an ARRAY, its data buffer is supposed to be empty,
    // so we don't resize it here
    if (ArrayType::getChildType(&type)->getLogicalTypeID() != LogicalTypeID::LIST &&
        ArrayType::getChildType(&type)->getLogicalTypeID() != LogicalTypeID::ARRAY) {
        vector->childData[0]->data.resize(numChildElements * storage::StorageUtils::getDataTypeSize(
                                                                 *ArrayType::getChildType(&type)));
    }
    for (auto i = 0u; i < numElements; i++) {
        appendValue(vector->childData[0].get(), *ArrayType::getChildType(&type),
            value->children[i].get());
    }
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::STRUCT>(ArrowVector* vector,
    const LogicalType& type, Value* value, std::int64_t /*pos*/) {
    for (auto i = 0u; i < value->childrenSize; i++) {
        appendValue(vector->childData[i].get(), *StructType::getFieldTypes(&type)[i],
            value->children[i].get());
    }
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::INTERNAL_ID>(ArrowVector* vector,
    const LogicalType& /*type*/, Value* value, std::int64_t /*pos*/) {
    auto nodeID = value->getValue<nodeID_t>();
    Value offsetVal((std::int64_t)nodeID.offset);
    Value tableIDVal((std::int64_t)nodeID.tableID);
    appendValue(vector->childData[0].get(), *LogicalType::INT64(), &offsetVal);
    appendValue(vector->childData[1].get(), *LogicalType::INT64(), &tableIDVal);
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::NODE>(ArrowVector* vector,
    const LogicalType& type, Value* value, std::int64_t /*pos*/) {
    appendValue(vector->childData[0].get(), *StructType::getFieldTypes(&type)[0],
        NodeVal::getNodeIDVal(value));
    appendValue(vector->childData[1].get(), *StructType::getFieldTypes(&type)[1],
        NodeVal::getLabelVal(value));
    std::int64_t propertyId = 2;
    auto numProperties = NodeVal::getNumProperties(value);
    for (auto i = 0u; i < numProperties; i++) {
        auto val = NodeVal::getPropertyVal(value, i);
        appendValue(vector->childData[propertyId].get(),
            *StructType::getFieldTypes(&type)[propertyId], val);
        propertyId++;
    }
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::REL>(ArrowVector* vector,
    const LogicalType& type, Value* value, std::int64_t /*pos*/) {
    appendValue(vector->childData[0].get(), *StructType::getFieldTypes(&type)[0],
        RelVal::getSrcNodeIDVal(value));
    appendValue(vector->childData[1].get(), *StructType::getFieldTypes(&type)[1],
        RelVal::getDstNodeIDVal(value));
    std::int64_t propertyId = 2;
    auto numProperties = NodeVal::getNumProperties(value);
    for (auto i = 0u; i < numProperties; i++) {
        auto val = NodeVal::getPropertyVal(value, i);
        appendValue(vector->childData[propertyId].get(),
            *StructType::getFieldTypes(&type)[propertyId], val);
        propertyId++;
    }
}

void ArrowRowBatch::copyNonNullValue(ArrowVector* vector, const LogicalType& type, Value* value,
    std::int64_t pos) {
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        templateCopyNonNullValue<LogicalTypeID::BOOL>(vector, type, value, pos);
    } break;
    case LogicalTypeID::INT128: {
        templateCopyNonNullValue<LogicalTypeID::INT128>(vector, type, value, pos);
    } break;
    case LogicalTypeID::UUID: {
        templateCopyNonNullValue<LogicalTypeID::UUID>(vector, type, value, pos);
    } break;
    case LogicalTypeID::INT64: {
        templateCopyNonNullValue<LogicalTypeID::INT64>(vector, type, value, pos);
    } break;
    case LogicalTypeID::INT32: {
        templateCopyNonNullValue<LogicalTypeID::INT32>(vector, type, value, pos);
    } break;
    case LogicalTypeID::INT16: {
        templateCopyNonNullValue<LogicalTypeID::INT16>(vector, type, value, pos);
    } break;
    case LogicalTypeID::INT8: {
        templateCopyNonNullValue<LogicalTypeID::INT8>(vector, type, value, pos);
    } break;
    case LogicalTypeID::UINT64: {
        templateCopyNonNullValue<LogicalTypeID::UINT64>(vector, type, value, pos);
    } break;
    case LogicalTypeID::UINT32: {
        templateCopyNonNullValue<LogicalTypeID::UINT32>(vector, type, value, pos);
    } break;
    case LogicalTypeID::UINT16: {
        templateCopyNonNullValue<LogicalTypeID::UINT16>(vector, type, value, pos);
    } break;
    case LogicalTypeID::UINT8: {
        templateCopyNonNullValue<LogicalTypeID::UINT8>(vector, type, value, pos);
    } break;
    case LogicalTypeID::DOUBLE: {
        templateCopyNonNullValue<LogicalTypeID::DOUBLE>(vector, type, value, pos);
    } break;
    case LogicalTypeID::FLOAT: {
        templateCopyNonNullValue<LogicalTypeID::FLOAT>(vector, type, value, pos);
    } break;
    case LogicalTypeID::DATE: {
        templateCopyNonNullValue<LogicalTypeID::DATE>(vector, type, value, pos);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        templateCopyNonNullValue<LogicalTypeID::TIMESTAMP>(vector, type, value, pos);
    } break;
    case LogicalTypeID::TIMESTAMP_TZ: {
        templateCopyNonNullValue<LogicalTypeID::TIMESTAMP_TZ>(vector, type, value, pos);
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        templateCopyNonNullValue<LogicalTypeID::TIMESTAMP_NS>(vector, type, value, pos);
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        templateCopyNonNullValue<LogicalTypeID::TIMESTAMP_MS>(vector, type, value, pos);
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        templateCopyNonNullValue<LogicalTypeID::TIMESTAMP_SEC>(vector, type, value, pos);
    } break;
    case LogicalTypeID::INTERVAL: {
        templateCopyNonNullValue<LogicalTypeID::INTERVAL>(vector, type, value, pos);
    } break;
    case LogicalTypeID::STRING: {
        templateCopyNonNullValue<LogicalTypeID::STRING>(vector, type, value, pos);
    } break;
    case LogicalTypeID::LIST: {
        templateCopyNonNullValue<LogicalTypeID::LIST>(vector, type, value, pos);
    } break;
    case LogicalTypeID::ARRAY: {
        templateCopyNonNullValue<LogicalTypeID::ARRAY>(vector, type, value, pos);
    } break;
    case LogicalTypeID::STRUCT: {
        templateCopyNonNullValue<LogicalTypeID::STRUCT>(vector, type, value, pos);
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        templateCopyNonNullValue<LogicalTypeID::INTERNAL_ID>(vector, type, value, pos);
    } break;
    case LogicalTypeID::NODE: {
        templateCopyNonNullValue<LogicalTypeID::NODE>(vector, type, value, pos);
    } break;
    case LogicalTypeID::REL: {
        templateCopyNonNullValue<LogicalTypeID::REL>(vector, type, value, pos);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

template<LogicalTypeID DT>
void ArrowRowBatch::templateCopyNullValue(ArrowVector* vector, std::int64_t pos) {
    // TODO(Guodong): make this as a function.
    setBitToZero(vector->validity.data(), pos);
    vector->numNulls++;
}

template<>
void ArrowRowBatch::templateCopyNullValue<LogicalTypeID::STRING>(ArrowVector* vector,
    std::int64_t pos) {
    auto offsets = (std::uint32_t*)vector->data.data();
    offsets[pos + 1] = offsets[pos];
    setBitToZero(vector->validity.data(), pos);
    vector->numNulls++;
}

template<>
void ArrowRowBatch::templateCopyNullValue<LogicalTypeID::LIST>(ArrowVector* vector,
    std::int64_t pos) {
    auto offsets = (std::uint32_t*)vector->data.data();
    offsets[pos + 1] = offsets[pos];
    setBitToZero(vector->validity.data(), pos);
    vector->numNulls++;
}

template<>
void ArrowRowBatch::templateCopyNullValue<LogicalTypeID::ARRAY>(ArrowVector* vector,
    std::int64_t pos) {
    setBitToZero(vector->validity.data(), pos);
    vector->numNulls++;
}

template<>
void ArrowRowBatch::templateCopyNullValue<LogicalTypeID::STRUCT>(ArrowVector* vector,
    std::int64_t pos) {
    setBitToZero(vector->validity.data(), pos);
    vector->numNulls++;
}

void ArrowRowBatch::copyNullValue(ArrowVector* vector, Value* value, std::int64_t pos) {
    switch (value->dataType->getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        templateCopyNullValue<LogicalTypeID::BOOL>(vector, pos);
    } break;
    case LogicalTypeID::INT128: {
        templateCopyNullValue<LogicalTypeID::INT128>(vector, pos);
    } break;
    case LogicalTypeID::INT64: {
        templateCopyNullValue<LogicalTypeID::INT64>(vector, pos);
    } break;
    case LogicalTypeID::INT32: {
        templateCopyNullValue<LogicalTypeID::INT32>(vector, pos);
    } break;
    case LogicalTypeID::INT16: {
        templateCopyNullValue<LogicalTypeID::INT16>(vector, pos);
    } break;
    case LogicalTypeID::INT8: {
        templateCopyNullValue<LogicalTypeID::INT8>(vector, pos);
    } break;
    case LogicalTypeID::UINT64: {
        templateCopyNullValue<LogicalTypeID::UINT64>(vector, pos);
    } break;
    case LogicalTypeID::UINT32: {
        templateCopyNullValue<LogicalTypeID::UINT32>(vector, pos);
    } break;
    case LogicalTypeID::UINT16: {
        templateCopyNullValue<LogicalTypeID::UINT16>(vector, pos);
    } break;
    case LogicalTypeID::UINT8: {
        templateCopyNullValue<LogicalTypeID::UINT8>(vector, pos);
    } break;
    case LogicalTypeID::DOUBLE: {
        templateCopyNullValue<LogicalTypeID::DOUBLE>(vector, pos);
    } break;
    case LogicalTypeID::FLOAT: {
        templateCopyNullValue<LogicalTypeID::FLOAT>(vector, pos);
    } break;
    case LogicalTypeID::DATE: {
        templateCopyNullValue<LogicalTypeID::DATE>(vector, pos);
    } break;
    case LogicalTypeID::TIMESTAMP_MS: {
        templateCopyNullValue<LogicalTypeID::TIMESTAMP_MS>(vector, pos);
    } break;
    case LogicalTypeID::TIMESTAMP_NS: {
        templateCopyNullValue<LogicalTypeID::TIMESTAMP_NS>(vector, pos);
    } break;
    case LogicalTypeID::TIMESTAMP_SEC: {
        templateCopyNullValue<LogicalTypeID::TIMESTAMP_SEC>(vector, pos);
    } break;
    case LogicalTypeID::TIMESTAMP_TZ: {
        templateCopyNullValue<LogicalTypeID::TIMESTAMP_TZ>(vector, pos);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        templateCopyNullValue<LogicalTypeID::TIMESTAMP>(vector, pos);
    } break;
    case LogicalTypeID::INTERVAL: {
        templateCopyNullValue<LogicalTypeID::INTERVAL>(vector, pos);
    } break;
    case LogicalTypeID::UUID:
    case LogicalTypeID::STRING: {
        templateCopyNullValue<LogicalTypeID::STRING>(vector, pos);
    } break;
    case LogicalTypeID::LIST: {
        templateCopyNullValue<LogicalTypeID::LIST>(vector, pos);
    } break;
    case LogicalTypeID::ARRAY: {
        templateCopyNullValue<LogicalTypeID::ARRAY>(vector, pos);
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        templateCopyNullValue<LogicalTypeID::INTERNAL_ID>(vector, pos);
    } break;
    case LogicalTypeID::STRUCT: {
        templateCopyNullValue<LogicalTypeID::STRUCT>(vector, pos);
    } break;
    case LogicalTypeID::NODE: {
        templateCopyNullValue<LogicalTypeID::NODE>(vector, pos);
    } break;
    case LogicalTypeID::REL: {
        templateCopyNullValue<LogicalTypeID::REL>(vector, pos);
    } break;
    default: {
        KU_UNREACHABLE;
    }
    }
}

static void releaseArrowVector(ArrowArray* array) {
    if (!array || !array->release) {
        return;
    }
    array->release = nullptr;
    auto holder = static_cast<ArrowVector*>(array->private_data);
    delete holder;
}

static std::unique_ptr<ArrowArray> createArrayFromVector(ArrowVector& vector) {
    auto result = std::make_unique<ArrowArray>();
    result->private_data = nullptr;
    result->release = releaseArrowVector;
    result->n_children = 0;
    result->offset = 0;
    result->dictionary = nullptr;
    result->buffers = vector.buffers.data();
    result->null_count = vector.numNulls;
    result->length = vector.numValues;
    result->n_buffers = 1;
    result->buffers[0] = vector.validity.data();
    if (vector.data.data() != nullptr) {
        result->n_buffers++;
        result->buffers[1] = vector.data.data();
    }
    return result;
}

template<LogicalTypeID DT>
ArrowArray* ArrowRowBatch::templateCreateArray(ArrowVector& vector, const LogicalType& /*type*/) {
    auto result = createArrayFromVector(vector);
    vector.array = std::move(result);
    return vector.array.get();
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<LogicalTypeID::STRING>(ArrowVector& vector,
    const LogicalType& /*type*/) {
    auto result = createArrayFromVector(vector);
    result->n_buffers = 3;
    result->buffers[2] = vector.overflow.data();
    vector.array = std::move(result);
    return vector.array.get();
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<LogicalTypeID::LIST>(ArrowVector& vector,
    const LogicalType& type) {
    auto result = createArrayFromVector(vector);
    vector.childPointers.resize(1);
    result->children = vector.childPointers.data();
    result->n_children = 1;
    vector.childPointers[0] =
        convertVectorToArray(*vector.childData[0], *ListType::getChildType(&type));
    vector.array = std::move(result);
    return vector.array.get();
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<LogicalTypeID::ARRAY>(ArrowVector& vector,
    const LogicalType& type) {
    auto result = createArrayFromVector(vector);
    vector.childPointers.resize(1);
    result->n_buffers = 1;
    result->children = vector.childPointers.data();
    result->n_children = 1;
    vector.childPointers[0] =
        convertVectorToArray(*vector.childData[0], *ArrayType::getChildType(&type));
    vector.array = std::move(result);
    return vector.array.get();
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<LogicalTypeID::STRUCT>(ArrowVector& vector,
    const LogicalType& type) {
    return convertStructVectorToArray(vector, type);
}

ArrowArray* ArrowRowBatch::convertStructVectorToArray(ArrowVector& vector,
    const LogicalType& type) {
    auto result = createArrayFromVector(vector);
    result->n_buffers = 1;
    vector.childPointers.resize(StructType::getNumFields(&type));
    result->children = vector.childPointers.data();
    result->n_children = (std::int64_t)StructType::getNumFields(&type);
    for (auto i = 0u; i < StructType::getNumFields(&type); i++) {
        auto& childType = *StructType::getFieldTypes(&type)[i];
        vector.childPointers[i] = convertVectorToArray(*vector.childData[i], childType);
    }
    vector.array = std::move(result);
    return vector.array.get();
}

ArrowArray* ArrowRowBatch::convertInternalIDVectorToArray(ArrowVector& vector,
    const LogicalType& /*type*/) {
    auto result = createArrayFromVector(vector);
    result->n_buffers = 1;
    vector.childPointers.resize(2);
    result->children = vector.childPointers.data();
    result->n_children = 2;
    for (auto i = 0; i < 2; i++) {
        auto childType = *LogicalType::INT64();
        vector.childPointers[i] = convertVectorToArray(*vector.childData[i], childType);
    }
    vector.array = std::move(result);
    return vector.array.get();
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<LogicalTypeID::INTERNAL_ID>(ArrowVector& vector,
    const LogicalType& type) {
    return convertInternalIDVectorToArray(vector, type);
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<LogicalTypeID::NODE>(ArrowVector& vector,
    const LogicalType& type) {
    return convertStructVectorToArray(vector, type);
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<LogicalTypeID::REL>(ArrowVector& vector,
    const LogicalType& type) {
    return convertStructVectorToArray(vector, type);
}

ArrowArray* ArrowRowBatch::convertVectorToArray(ArrowVector& vector, const LogicalType& type) {
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        return templateCreateArray<LogicalTypeID::BOOL>(vector, type);
    }
    case LogicalTypeID::INT128: {
        return templateCreateArray<LogicalTypeID::INT128>(vector, type);
    }
    case LogicalTypeID::INT64: {
        return templateCreateArray<LogicalTypeID::INT64>(vector, type);
    }
    case LogicalTypeID::INT32: {
        return templateCreateArray<LogicalTypeID::INT32>(vector, type);
    }
    case LogicalTypeID::INT16: {
        return templateCreateArray<LogicalTypeID::INT16>(vector, type);
    }
    case LogicalTypeID::INT8: {
        return templateCreateArray<LogicalTypeID::INT8>(vector, type);
    }
    case LogicalTypeID::UINT64: {
        return templateCreateArray<LogicalTypeID::UINT64>(vector, type);
    }
    case LogicalTypeID::UINT32: {
        return templateCreateArray<LogicalTypeID::UINT32>(vector, type);
    }
    case LogicalTypeID::UINT16: {
        return templateCreateArray<LogicalTypeID::UINT16>(vector, type);
    }
    case LogicalTypeID::UINT8: {
        return templateCreateArray<LogicalTypeID::UINT8>(vector, type);
    }
    case LogicalTypeID::DOUBLE: {
        return templateCreateArray<LogicalTypeID::DOUBLE>(vector, type);
    }
    case LogicalTypeID::FLOAT: {
        return templateCreateArray<LogicalTypeID::FLOAT>(vector, type);
    }
    case LogicalTypeID::DATE: {
        return templateCreateArray<LogicalTypeID::DATE>(vector, type);
    }
    case LogicalTypeID::TIMESTAMP_MS: {
        return templateCreateArray<LogicalTypeID::TIMESTAMP_MS>(vector, type);
    }
    case LogicalTypeID::TIMESTAMP_NS: {
        return templateCreateArray<LogicalTypeID::TIMESTAMP_NS>(vector, type);
    }
    case LogicalTypeID::TIMESTAMP_SEC: {
        return templateCreateArray<LogicalTypeID::TIMESTAMP_SEC>(vector, type);
    }
    case LogicalTypeID::TIMESTAMP_TZ: {
        return templateCreateArray<LogicalTypeID::TIMESTAMP_TZ>(vector, type);
    }
    case LogicalTypeID::TIMESTAMP: {
        return templateCreateArray<LogicalTypeID::TIMESTAMP>(vector, type);
    }
    case LogicalTypeID::INTERVAL: {
        return templateCreateArray<LogicalTypeID::INTERVAL>(vector, type);
    }
    case LogicalTypeID::UUID:
    case LogicalTypeID::STRING: {
        return templateCreateArray<LogicalTypeID::STRING>(vector, type);
    }
    case LogicalTypeID::LIST: {
        return templateCreateArray<LogicalTypeID::LIST>(vector, type);
    }
    case LogicalTypeID::ARRAY: {
        return templateCreateArray<LogicalTypeID::ARRAY>(vector, type);
    }
    case LogicalTypeID::STRUCT: {
        return templateCreateArray<LogicalTypeID::STRUCT>(vector, type);
    }
    case LogicalTypeID::INTERNAL_ID: {
        return templateCreateArray<LogicalTypeID::INTERNAL_ID>(vector, type);
    }
    case LogicalTypeID::NODE: {
        return templateCreateArray<LogicalTypeID::NODE>(vector, type);
    }
    case LogicalTypeID::REL: {
        return templateCreateArray<LogicalTypeID::REL>(vector, type);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

ArrowArray ArrowRowBatch::toArray() {
    auto rootHolder = std::make_unique<ArrowVector>();
    ArrowArray result;
    rootHolder->childPointers.resize(types.size());
    result.children = rootHolder->childPointers.data();
    result.n_children = (std::int64_t)types.size();
    result.length = numTuples;
    result.n_buffers = 1;
    result.buffers = rootHolder->buffers.data(); // no actual buffer
    result.offset = 0;
    result.null_count = 0;
    result.dictionary = nullptr;
    rootHolder->childData = std::move(vectors);
    for (auto i = 0u; i < rootHolder->childData.size(); i++) {
        rootHolder->childPointers[i] = convertVectorToArray(*rootHolder->childData[i], types[i]);
    }
    result.private_data = rootHolder.release();
    result.release = releaseArrowVector;
    return result;
}

ArrowArray ArrowRowBatch::append(main::QueryResult& queryResult, std::int64_t chunkSize) {
    std::int64_t numTuplesInBatch = 0;
    auto numColumns = queryResult.getColumnNames().size();
    while (numTuplesInBatch < chunkSize) {
        if (!queryResult.hasNext()) {
            break;
        }
        auto tuple = queryResult.getNext();
        for (auto i = 0u; i < numColumns; i++) {
            appendValue(vectors[i].get(), types[i], tuple->getValue(i));
        }
        numTuplesInBatch++;
    }
    numTuples += numTuplesInBatch;
    return toArray();
}

} // namespace common
} // namespace kuzu
