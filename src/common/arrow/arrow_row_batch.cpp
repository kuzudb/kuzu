#include "common/arrow/arrow_row_batch.h"

#include "common/types/value.h"

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

template<DataTypeID DT>
void ArrowRowBatch::templateInitializeVector(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    vector->data.reserve(Types::getDataTypeSize(DT) * capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<BOOL>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    vector->data.reserve(getNumBytesForBits(capacity));
}

template<>
void ArrowRowBatch::templateInitializeVector<STRING>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    // Initialize offsets and string values buffer.
    vector->data.reserve((capacity + 1) * sizeof(std::uint32_t));
    ((std::uint32_t*)vector->data.data())[0] = 0;
    vector->overflow.reserve(capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<VAR_LIST>(
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
void ArrowRowBatch::templateInitializeVector<INTERNAL_ID>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeStructVector(vector, typeInfo, capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<NODE>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeStructVector(vector, typeInfo, capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<REL>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    initializeStructVector(vector, typeInfo, capacity);
}

std::unique_ptr<ArrowVector> ArrowRowBatch::createVector(
    const main::DataTypeInfo& typeInfo, std::int64_t capacity) {
    auto result = std::make_unique<ArrowVector>();
    switch (typeInfo.typeID) {
    case BOOL: {
        templateInitializeVector<BOOL>(result.get(), typeInfo, capacity);
    } break;
    case INT64: {
        templateInitializeVector<INT64>(result.get(), typeInfo, capacity);
    } break;
    case DOUBLE: {
        templateInitializeVector<DOUBLE>(result.get(), typeInfo, capacity);
    } break;
    case DATE: {
        templateInitializeVector<DATE>(result.get(), typeInfo, capacity);
    } break;
    case TIMESTAMP: {
        templateInitializeVector<TIMESTAMP>(result.get(), typeInfo, capacity);
    } break;
    case INTERVAL: {
        templateInitializeVector<INTERVAL>(result.get(), typeInfo, capacity);
    } break;
    case STRING: {
        templateInitializeVector<STRING>(result.get(), typeInfo, capacity);
    } break;
    case VAR_LIST: {
        templateInitializeVector<VAR_LIST>(result.get(), typeInfo, capacity);
    } break;
    case INTERNAL_ID: {
        templateInitializeVector<INTERNAL_ID>(result.get(), typeInfo, capacity);
    } break;
    case NODE: {
        templateInitializeVector<NODE>(result.get(), typeInfo, capacity);
    } break;
    case REL: {
        templateInitializeVector<REL>(result.get(), typeInfo, capacity);
    } break;
    default: {
        throw RuntimeException(
            "Invalid data type " + Types::dataTypeToString(typeInfo.typeID) + " for arrow export.");
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

template<DataTypeID DT>
void ArrowRowBatch::templateCopyNonNullValue(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    auto valSize = Types::getDataTypeSize(DT);
    std::memcpy(vector->data.data() + pos * valSize, &value->val, valSize);
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<BOOL>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    if (value->val.booleanVal) {
        setBitToOne(vector->data.data(), pos);
    } else {
        setBitToZero(vector->data.data(), pos);
    }
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<STRING>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    auto offsets = (std::uint32_t*)vector->data.data();
    auto strLength = value->strVal.length();
    offsets[pos + 1] = offsets[pos] + strLength;
    vector->overflow.resize(offsets[pos + 1]);
    std::memcpy(vector->overflow.data() + offsets[pos], value->strVal.data(), strLength);
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<VAR_LIST>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    vector->data.resize((pos + 2) * sizeof(std::uint32_t));
    auto offsets = (std::uint32_t*)vector->data.data();
    auto numElements = value->listVal.size();
    offsets[pos + 1] = offsets[pos] + numElements;
    auto numChildElements = offsets[pos + 1] + 1;
    auto currentNumBytesForChildValidity = vector->childData[0]->validity.size();
    auto numBytesForChildValidity = getNumBytesForBits(numChildElements);
    vector->childData[0]->validity.resize(numBytesForChildValidity);
    // Initialize validity mask which is used to mark each value is valid (non-null) or not (null).
    for (auto i = currentNumBytesForChildValidity; i < numBytesForChildValidity; i++) {
        vector->childData[0]->validity.data()[i] = 0xFF; // Init each value to be valid (as 1).
    }
    if (typeInfo.childrenTypesInfo[0]->typeID != VAR_LIST) {
        vector->childData[0]->data.resize(
            numChildElements * Types::getDataTypeSize(typeInfo.childrenTypesInfo[0]->typeID));
    }
    for (auto i = 0u; i < numElements; i++) {
        appendValue(
            vector->childData[0].get(), *typeInfo.childrenTypesInfo[0], value->listVal[i].get());
    }
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<INTERNAL_ID>(
    ArrowVector* vector, const main::DataTypeInfo& typeInfo, Value* value, std::int64_t pos) {
    auto nodeID = value->getValue<nodeID_t>();
    Value offsetVal((std::int64_t)nodeID.offset);
    Value tableIDVal((std::int64_t)nodeID.tableID);
    appendValue(vector->childData[0].get(), *typeInfo.childrenTypesInfo[0], &offsetVal);
    appendValue(vector->childData[1].get(), *typeInfo.childrenTypesInfo[1], &tableIDVal);
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<NODE>(
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
void ArrowRowBatch::templateCopyNonNullValue<REL>(
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
    case BOOL: {
        templateCopyNonNullValue<BOOL>(vector, typeInfo, value, pos);
    } break;
    case INT64: {
        templateCopyNonNullValue<INT64>(vector, typeInfo, value, pos);
    } break;
    case DOUBLE: {
        templateCopyNonNullValue<DOUBLE>(vector, typeInfo, value, pos);
    } break;
    case DATE: {
        templateCopyNonNullValue<DATE>(vector, typeInfo, value, pos);
    } break;
    case TIMESTAMP: {
        templateCopyNonNullValue<TIMESTAMP>(vector, typeInfo, value, pos);
    } break;
    case INTERVAL: {
        templateCopyNonNullValue<INTERVAL>(vector, typeInfo, value, pos);
    } break;
    case STRING: {
        templateCopyNonNullValue<STRING>(vector, typeInfo, value, pos);
    } break;
    case VAR_LIST: {
        templateCopyNonNullValue<VAR_LIST>(vector, typeInfo, value, pos);
    } break;
    case INTERNAL_ID: {
        templateCopyNonNullValue<INTERNAL_ID>(vector, typeInfo, value, pos);
    } break;
    case NODE: {
        templateCopyNonNullValue<NODE>(vector, typeInfo, value, pos);
    } break;
    case REL: {
        templateCopyNonNullValue<REL>(vector, typeInfo, value, pos);
    } break;
    default: {
        throw RuntimeException(
            "Invalid data type " + Types::dataTypeToString(value->dataType) + " for arrow export.");
    }
    }
}

template<DataTypeID DT>
void ArrowRowBatch::templateCopyNullValue(ArrowVector* vector, std::int64_t pos) {
    // TODO(Guodong): make this as a function.
    setBitToZero(vector->validity.data(), pos);
    vector->numNulls++;
}

template<>
void ArrowRowBatch::templateCopyNullValue<STRING>(ArrowVector* vector, std::int64_t pos) {
    auto offsets = (std::uint32_t*)vector->data.data();
    offsets[pos + 1] = offsets[pos];
    setBitToZero(vector->validity.data(), pos);
    vector->numNulls++;
}

template<>
void ArrowRowBatch::templateCopyNullValue<VAR_LIST>(ArrowVector* vector, std::int64_t pos) {
    auto offsets = (std::uint32_t*)vector->data.data();
    offsets[pos + 1] = offsets[pos];
    setBitToZero(vector->validity.data(), pos);
    vector->numNulls++;
}

void ArrowRowBatch::copyNullValue(ArrowVector* vector, Value* value, std::int64_t pos) {
    switch (value->dataType.typeID) {
    case BOOL: {
        templateCopyNullValue<BOOL>(vector, pos);
    } break;
    case INT64: {
        templateCopyNullValue<INT64>(vector, pos);
    } break;
    case DOUBLE: {
        templateCopyNullValue<DOUBLE>(vector, pos);
    } break;
    case DATE: {
        templateCopyNullValue<DATE>(vector, pos);
    } break;
    case TIMESTAMP: {
        templateCopyNullValue<TIMESTAMP>(vector, pos);
    } break;
    case INTERVAL: {
        templateCopyNullValue<INTERVAL>(vector, pos);
    } break;
    case STRING: {
        templateCopyNullValue<STRING>(vector, pos);
    } break;
    case VAR_LIST: {
        templateCopyNullValue<VAR_LIST>(vector, pos);
    } break;
    case INTERNAL_ID: {
        templateCopyNullValue<INTERNAL_ID>(vector, pos);
    } break;
    case NODE: {
        templateCopyNullValue<NODE>(vector, pos);
    } break;
    case REL: {
        templateCopyNullValue<REL>(vector, pos);
    } break;
    default: {
        throw RuntimeException(
            "Invalid data type " + Types::dataTypeToString(value->dataType) + " for arrow export.");
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

template<DataTypeID DT>
ArrowArray* ArrowRowBatch::templateCreateArray(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    auto result = createArrayFromVector(vector);
    vector.array = std::move(result);
    return vector.array.get();
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<STRING>(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    auto result = createArrayFromVector(vector);
    result->n_buffers = 3;
    result->buffers[2] = vector.overflow.data();
    vector.array = std::move(result);
    return vector.array.get();
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<VAR_LIST>(
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
ArrowArray* ArrowRowBatch::templateCreateArray<INTERNAL_ID>(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    return convertStructVectorToArray(vector, typeInfo);
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<NODE>(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    return convertStructVectorToArray(vector, typeInfo);
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<REL>(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    return convertStructVectorToArray(vector, typeInfo);
}

ArrowArray* ArrowRowBatch::convertVectorToArray(
    ArrowVector& vector, const main::DataTypeInfo& typeInfo) {
    switch (typeInfo.typeID) {
    case BOOL: {
        return templateCreateArray<BOOL>(vector, typeInfo);
    }
    case INT64: {
        return templateCreateArray<INT64>(vector, typeInfo);
    }
    case DOUBLE: {
        return templateCreateArray<DOUBLE>(vector, typeInfo);
    }
    case DATE: {
        return templateCreateArray<DATE>(vector, typeInfo);
    }
    case TIMESTAMP: {
        return templateCreateArray<TIMESTAMP>(vector, typeInfo);
    }
    case INTERVAL: {
        return templateCreateArray<INTERVAL>(vector, typeInfo);
    }
    case STRING: {
        return templateCreateArray<STRING>(vector, typeInfo);
    }
    case VAR_LIST: {
        return templateCreateArray<VAR_LIST>(vector, typeInfo);
    }
    case INTERNAL_ID: {
        return templateCreateArray<INTERNAL_ID>(vector, typeInfo);
    }
    case NODE: {
        return templateCreateArray<NODE>(vector, typeInfo);
    }
    case REL: {
        return templateCreateArray<REL>(vector, typeInfo);
    }
    default: {
        throw RuntimeException(
            "Invalid data type " + Types::dataTypeToString(typeInfo.typeID) + " for arrow export.");
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
        std::vector<std::uint32_t> colWidths(numColumns, 10);
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
