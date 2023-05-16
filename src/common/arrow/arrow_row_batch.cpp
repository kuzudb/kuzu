#include "common/arrow/arrow_row_batch.h"

#include "common/types/value.h"
#include "storage/storage_utils.h"

namespace kuzu {
namespace common {

ArrowRowBatch::ArrowRowBatch(
    std::vector<std::unique_ptr<main::DataTypeInfo>> typesInfo, std::int64_t capacity)
    : typesInfo{std::move(typesInfo)}, numTuples{0} {
    auto numVectors = this->typesInfo.size();
    vectors.resize(numVectors);
    for (auto i = 0u; i < numVectors; i++) {
        vectors[i] = createVector(*this->typesInfo[i], capacity);
    }
}

// TODO(Ziyi): use physical type instead of logical type here.
template<LogicalTypeID DT>
void ArrowRowBatch::templateInitializeVector(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    vector->data.reserve(storage::StorageUtils::getDataTypeSize(LogicalType{DT}) * capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::BOOL>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    vector->data.reserve(getNumBytesForBits(capacity));
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::STRING>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    // Initialize offsets and string values buffer.
    vector->data.reserve((capacity + 1) * sizeof(std::uint32_t));
    ((std::uint32_t*)vector->data.data())[0] = 0;
    vector->overflow.reserve(capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::VAR_LIST>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    assert(typeInfo.childrenTypesInfo.size() == 1);
    auto childTypeInfo = typeInfo.childrenTypesInfo[0].get();
    // Initialize offsets and child buffer.
    vector->data.reserve((capacity + 1) * sizeof(std::uint32_t));
    ((std::uint32_t*)vector->data.data())[0] = 0;
    auto childVector = createVector(*childTypeInfo, capacity);
    vector->childData.push_back(std::move(childVector));
}

void ArrowRowBatch::initializeStructVector(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    for (auto& childTypeInfo : typeInfo.childrenTypesInfo) {
        auto childVector = createVector(*childTypeInfo, capacity);
        vector->childData.push_back(std::move(childVector));
    }
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::INTERNAL_ID>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeStructVector(vector, typeInfo, capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::NODE>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeStructVector(vector, typeInfo, capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<LogicalTypeID::REL>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeStructVector(vector, typeInfo, capacity);
}

std::unique_ptr<ArrowVector> ArrowRowBatch::createVector(
    const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    auto result = std::make_unique<ArrowVector>();
    switch (typeInfo.typeID) {
    case LogicalTypeID::BOOL: {
        templateInitializeVector<LogicalTypeID::BOOL>(result.get(), typeInfo, capacity);
    } break;
    case LogicalTypeID::INT64: {
        templateInitializeVector<LogicalTypeID::INT64>(result.get(), typeInfo, capacity);
    } break;
    case LogicalTypeID::INT32: {
        templateInitializeVector<LogicalTypeID::INT32>(result.get(), typeInfo, capacity);
    } break;
    case LogicalTypeID::INT16: {
        templateInitializeVector<LogicalTypeID::INT16>(result.get(), typeInfo, capacity);
    } break;
    case LogicalTypeID::DOUBLE: {
        templateInitializeVector<LogicalTypeID::DOUBLE>(result.get(), typeInfo, capacity);
    } break;
    case LogicalTypeID::DATE: {
        templateInitializeVector<LogicalTypeID::DATE>(result.get(), typeInfo, capacity);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        templateInitializeVector<LogicalTypeID::TIMESTAMP>(result.get(), typeInfo, capacity);
    } break;
    case LogicalTypeID::INTERVAL: {
        templateInitializeVector<LogicalTypeID::INTERVAL>(result.get(), typeInfo, capacity);
    } break;
    case LogicalTypeID::STRING: {
        templateInitializeVector<LogicalTypeID::STRING>(result.get(), typeInfo, capacity);
    } break;
    case LogicalTypeID::VAR_LIST: {
        templateInitializeVector<LogicalTypeID::VAR_LIST>(result.get(), typeInfo, capacity);
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        templateInitializeVector<LogicalTypeID::INTERNAL_ID>(result.get(), typeInfo, capacity);
    } break;
    case LogicalTypeID::NODE: {
        templateInitializeVector<LogicalTypeID::NODE>(result.get(), typeInfo, capacity);
    } break;
    case LogicalTypeID::REL: {
        templateInitializeVector<LogicalTypeID::REL>(result.get(), typeInfo, capacity);
    } break;
    default: {
        throw RuntimeException("Invalid data type " +
                               LogicalTypeUtils::dataTypeToString(typeInfo.typeID) +
                               " for arrow export.");
    }
    }
    return std::move(result);
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

void ArrowRowBatch::appendValue(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value) {
    if (value->isNull_) {
        copyNullValue(vector, value, vector->numValues);
    } else {
        copyNonNullValue(vector, typeInfo, value, vector->numValues);
    }
    vector->numValues++;
}

template<LogicalTypeID DT>
void ArrowRowBatch::templateCopyNonNullValue(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    auto valSize = storage::StorageUtils::getDataTypeSize(LogicalType{DT});
    std::memcpy(vector->data.data() + pos * valSize, &value->val, valSize);
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::BOOL>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    if (value->val.booleanVal) {
        setBitToOne(vector->data.data(), pos);
    } else {
        setBitToZero(vector->data.data(), pos);
    }
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::STRING>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    auto offsets = (std::uint32_t*)vector->data.data();
    auto strLength = value->strVal.length();
    offsets[pos + 1] = offsets[pos] + strLength;
    vector->overflow.resize(offsets[pos + 1]);
    std::memcpy(vector->overflow.data() + offsets[pos], value->strVal.data(), strLength);
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::VAR_LIST>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    vector->data.resize((pos + 2) * sizeof(std::uint32_t));
    auto offsets = (std::uint32_t*)vector->data.data();
    auto numElements = value->nestedTypeVal.size();
    offsets[pos + 1] = offsets[pos] + numElements;
    auto numChildElements = offsets[pos + 1] + 1;
    auto currentNumBytesForChildValidity = vector->childData[0]->validity.size();
    auto numBytesForChildValidity = getNumBytesForBits(numChildElements);
    vector->childData[0]->validity.resize(numBytesForChildValidity);
    // Initialize validity mask which is used to mark each value is valid (non-null) or not (null).
    for (auto i = currentNumBytesForChildValidity; i < numBytesForChildValidity; i++) {
        vector->childData[0]->validity.data()[i] = 0xFF; // Init each value to be valid (as 1).
    }
    if (typeInfo.childrenTypesInfo[0]->typeID != LogicalTypeID::VAR_LIST) {
        vector->childData[0]->data.resize(
            numChildElements * storage::StorageUtils::getDataTypeSize(
                                   LogicalType{typeInfo.childrenTypesInfo[0]->typeID}));
    }
    for (auto i = 0u; i < numElements; i++) {
        appendValue(vector->childData[0].get(), *typeInfo.childrenTypesInfo[0],
            value->nestedTypeVal[i].get());
    }
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::INTERNAL_ID>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    auto nodeID = value->getValue<nodeID_t>();
    Value offsetVal((std::int64_t)nodeID.offset);
    Value tableIDVal((std::int64_t)nodeID.tableID);
    appendValue(vector->childData[0].get(), *typeInfo.childrenTypesInfo[0], &offsetVal);
    appendValue(vector->childData[1].get(), *typeInfo.childrenTypesInfo[1], &tableIDVal);
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::NODE>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    appendValue(
        vector->childData[0].get(), *typeInfo.childrenTypesInfo[0], value->nodeVal->getNodeIDVal());
    appendValue(
        vector->childData[1].get(), *typeInfo.childrenTypesInfo[1], value->nodeVal->getLabelVal());
    std::int64_t propertyId = 2;
    for (auto& [name, val] : value->nodeVal->getProperties()) {
        appendValue(vector->childData[propertyId].get(), *typeInfo.childrenTypesInfo[propertyId],
            val.get());
        propertyId++;
    }
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<LogicalTypeID::REL>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    appendValue(vector->childData[0].get(), *typeInfo.childrenTypesInfo[0],
        value->relVal->getSrcNodeIDVal());
    appendValue(vector->childData[1].get(), *typeInfo.childrenTypesInfo[1],
        value->relVal->getDstNodeIDVal());
    std::int64_t propertyId = 2;
    for (auto& [name, val] : value->relVal->getProperties()) {
        appendValue(vector->childData[propertyId].get(), *typeInfo.childrenTypesInfo[propertyId],
            val.get());
        propertyId++;
    }
}

void ArrowRowBatch::copyNonNullValue(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    switch (typeInfo.typeID) {
    case LogicalTypeID::BOOL: {
        templateCopyNonNullValue<LogicalTypeID::BOOL>(vector, typeInfo, value, pos);
    } break;
    case LogicalTypeID::INT64: {
        templateCopyNonNullValue<LogicalTypeID::INT64>(vector, typeInfo, value, pos);
    } break;
    case LogicalTypeID::INT32: {
        templateCopyNonNullValue<LogicalTypeID::INT32>(vector, typeInfo, value, pos);
    } break;
    case LogicalTypeID::INT16: {
        templateCopyNonNullValue<LogicalTypeID::INT16>(vector, typeInfo, value, pos);
    } break;
    case LogicalTypeID::DOUBLE: {
        templateCopyNonNullValue<LogicalTypeID::DOUBLE>(vector, typeInfo, value, pos);
    } break;
    case LogicalTypeID::DATE: {
        templateCopyNonNullValue<LogicalTypeID::DATE>(vector, typeInfo, value, pos);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        templateCopyNonNullValue<LogicalTypeID::TIMESTAMP>(vector, typeInfo, value, pos);
    } break;
    case LogicalTypeID::INTERVAL: {
        templateCopyNonNullValue<LogicalTypeID::INTERVAL>(vector, typeInfo, value, pos);
    } break;
    case LogicalTypeID::STRING: {
        templateCopyNonNullValue<LogicalTypeID::STRING>(vector, typeInfo, value, pos);
    } break;
    case LogicalTypeID::VAR_LIST: {
        templateCopyNonNullValue<LogicalTypeID::VAR_LIST>(vector, typeInfo, value, pos);
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        templateCopyNonNullValue<LogicalTypeID::INTERNAL_ID>(vector, typeInfo, value, pos);
    } break;
    case LogicalTypeID::NODE: {
        templateCopyNonNullValue<LogicalTypeID::NODE>(vector, typeInfo, value, pos);
    } break;
    case LogicalTypeID::REL: {
        templateCopyNonNullValue<LogicalTypeID::REL>(vector, typeInfo, value, pos);
    } break;
    default: {
        throw RuntimeException("Invalid data type " +
                               LogicalTypeUtils::dataTypeToString(value->dataType) +
                               " for arrow export.");
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
void ArrowRowBatch::templateCopyNullValue<LogicalTypeID::STRING>(
    ArrowVector* vector, std::int64_t pos) {
    auto offsets = (std::uint32_t*)vector->data.data();
    offsets[pos + 1] = offsets[pos];
    setBitToZero(vector->validity.data(), pos);
    vector->numNulls++;
}

template<>
void ArrowRowBatch::templateCopyNullValue<LogicalTypeID::VAR_LIST>(
    ArrowVector* vector, std::int64_t pos) {
    auto offsets = (std::uint32_t*)vector->data.data();
    offsets[pos + 1] = offsets[pos];
    setBitToZero(vector->validity.data(), pos);
    vector->numNulls++;
}

void ArrowRowBatch::copyNullValue(ArrowVector* vector, Value* value, std::int64_t pos) {
    switch (value->dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        templateCopyNullValue<LogicalTypeID::BOOL>(vector, pos);
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
    case LogicalTypeID::DOUBLE: {
        templateCopyNullValue<LogicalTypeID::DOUBLE>(vector, pos);
    } break;
    case LogicalTypeID::DATE: {
        templateCopyNullValue<LogicalTypeID::DATE>(vector, pos);
    } break;
    case LogicalTypeID::TIMESTAMP: {
        templateCopyNullValue<LogicalTypeID::TIMESTAMP>(vector, pos);
    } break;
    case LogicalTypeID::INTERVAL: {
        templateCopyNullValue<LogicalTypeID::INTERVAL>(vector, pos);
    } break;
    case LogicalTypeID::STRING: {
        templateCopyNullValue<LogicalTypeID::STRING>(vector, pos);
    } break;
    case LogicalTypeID::VAR_LIST: {
        templateCopyNullValue<LogicalTypeID::VAR_LIST>(vector, pos);
    } break;
    case LogicalTypeID::INTERNAL_ID: {
        templateCopyNullValue<LogicalTypeID::INTERNAL_ID>(vector, pos);
    } break;
    case LogicalTypeID::NODE: {
        templateCopyNullValue<LogicalTypeID::NODE>(vector, pos);
    } break;
    case LogicalTypeID::REL: {
        templateCopyNullValue<LogicalTypeID::REL>(vector, pos);
    } break;
    default: {
        throw RuntimeException("Invalid data type " +
                               LogicalTypeUtils::dataTypeToString(value->dataType) +
                               " for arrow export.");
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
    result->n_buffers = 2;
    result->buffers[0] = vector.validity.data();
    result->buffers[1] = vector.data.data();
    return std::move(result);
}

template<LogicalTypeID DT>
ArrowArray* ArrowRowBatch::templateCreateArray(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    auto result = createArrayFromVector(vector);
    vector.array = std::move(result);
    return vector.array.get();
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<LogicalTypeID::STRING>(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    auto result = createArrayFromVector(vector);
    result->n_buffers = 3;
    result->buffers[2] = vector.overflow.data();
    vector.array = std::move(result);
    return vector.array.get();
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<LogicalTypeID::VAR_LIST>(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    auto result = createArrayFromVector(vector);
    vector.childPointers.resize(1);
    result->children = vector.childPointers.data();
    result->n_children = 1;
    vector.childPointers[0] =
        convertVectorToArray(*vector.childData[0], *typeInfo.childrenTypesInfo[0]);
    vector.array = std::move(result);
    return vector.array.get();
}

ArrowArray* ArrowRowBatch::convertStructVectorToArray(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    auto result = createArrayFromVector(vector);
    result->n_buffers = 1;
    vector.childPointers.resize(typeInfo.childrenTypesInfo.size());
    result->children = vector.childPointers.data();
    result->n_children = (std::int64_t)typeInfo.childrenTypesInfo.size();
    for (auto i = 0u; i < typeInfo.childrenTypesInfo.size(); i++) {
        auto& childTypeInfo = typeInfo.childrenTypesInfo[i];
        vector.childPointers[i] = convertVectorToArray(*vector.childData[i], *childTypeInfo);
    }
    vector.array = std::move(result);
    return vector.array.get();
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<LogicalTypeID::INTERNAL_ID>(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    return convertStructVectorToArray(vector, typeInfo);
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<LogicalTypeID::NODE>(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    return convertStructVectorToArray(vector, typeInfo);
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<LogicalTypeID::REL>(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    return convertStructVectorToArray(vector, typeInfo);
}

ArrowArray* ArrowRowBatch::convertVectorToArray(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    switch (typeInfo.typeID) {
    case LogicalTypeID::BOOL: {
        return templateCreateArray<LogicalTypeID::BOOL>(vector, typeInfo);
    }
    case LogicalTypeID::INT64: {
        return templateCreateArray<LogicalTypeID::INT64>(vector, typeInfo);
    }
    case LogicalTypeID::INT32: {
        return templateCreateArray<LogicalTypeID::INT32>(vector, typeInfo);
    }
    case LogicalTypeID::INT16: {
        return templateCreateArray<LogicalTypeID::INT16>(vector, typeInfo);
    }
    case LogicalTypeID::DOUBLE: {
        return templateCreateArray<LogicalTypeID::DOUBLE>(vector, typeInfo);
    }
    case LogicalTypeID::DATE: {
        return templateCreateArray<LogicalTypeID::DATE>(vector, typeInfo);
    }
    case LogicalTypeID::TIMESTAMP: {
        return templateCreateArray<LogicalTypeID::TIMESTAMP>(vector, typeInfo);
    }
    case LogicalTypeID::INTERVAL: {
        return templateCreateArray<LogicalTypeID::INTERVAL>(vector, typeInfo);
    }
    case LogicalTypeID::STRING: {
        return templateCreateArray<LogicalTypeID::STRING>(vector, typeInfo);
    }
    case LogicalTypeID::VAR_LIST: {
        return templateCreateArray<LogicalTypeID::VAR_LIST>(vector, typeInfo);
    }
    case LogicalTypeID::INTERNAL_ID: {
        return templateCreateArray<LogicalTypeID::INTERNAL_ID>(vector, typeInfo);
    }
    case LogicalTypeID::NODE: {
        return templateCreateArray<LogicalTypeID::NODE>(vector, typeInfo);
    }
    case LogicalTypeID::REL: {
        return templateCreateArray<LogicalTypeID::REL>(vector, typeInfo);
    }
    default: {
        throw RuntimeException("Invalid data type " +
                               LogicalTypeUtils::dataTypeToString(typeInfo.typeID) +
                               " for arrow export.");
    }
    }
}

ArrowArray ArrowRowBatch::toArray() {
    auto rootHolder = std::make_unique<ArrowVector>();
    ArrowArray result;
    rootHolder->childPointers.resize(typesInfo.size());
    result.children = rootHolder->childPointers.data();
    result.n_children = (std::int64_t)typesInfo.size();
    result.length = numTuples;
    result.n_buffers = 1;
    result.buffers = rootHolder->buffers.data(); // no actual buffer
    result.offset = 0;
    result.null_count = 0;
    result.dictionary = nullptr;
    rootHolder->childData = std::move(vectors);
    for (auto i = 0u; i < rootHolder->childData.size(); i++) {
        rootHolder->childPointers[i] =
            convertVectorToArray(*rootHolder->childData[i], *typesInfo[i]);
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
            appendValue(vectors[i].get(), *typesInfo[i], tuple->getValue(i));
        }
        numTuplesInBatch++;
    }
    numTuples += numTuplesInBatch;
    return toArray();
}

} // namespace common
} // namespace kuzu
