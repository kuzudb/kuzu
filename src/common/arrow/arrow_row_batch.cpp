#include "common/arrow/arrow_row_batch.h"

ArrowRowBatch::ArrowRowBatch(std::vector<DataType> types, std::int64_t capacity)
    : types{std::move(types)}, numTuples{0} {
    auto numVectors = this->types.size();
    vectors.resize(numVectors);
    for (auto i = 0u; i < numVectors; i++) {
        vectors[i] = createVector(this->types[i], capacity);
    }
}

template<typename T>
void ArrowRowBatch::templateInitializeVector(ArrowVector* vector, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    vector->data.reserve(sizeof(T) * capacity);
}

template<>
void ArrowRowBatch::templateInitializeVector<bool>(ArrowVector* vector, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    vector->data.reserve(getNumBytesForBits(capacity));
}

template<>
void ArrowRowBatch::templateInitializeVector<std::string>(
    ArrowVector* vector, std::int64_t capacity) {
    initializeNullBits(vector->validity, capacity);
    // Initialize offsets and string values buffer.
    vector->data.reserve((capacity + 1) * sizeof(std::uint32_t));
    ((std::uint32_t*)vector->data.data())[0] = 0;
    vector->overflow.reserve(capacity);
}

std::unique_ptr<ArrowVector> ArrowRowBatch::createVector(
    const DataType& type, std::int64_t capacity) {
    auto result = make_unique<ArrowVector>();
    switch (type.typeID) {
    case BOOL: {
        templateInitializeVector<bool>(result.get(), capacity);
    } break;
    case INT64: {
        templateInitializeVector<std::int64_t>(result.get(), capacity);
    } break;
    case DOUBLE: {
        templateInitializeVector<std::double_t>(result.get(), capacity);
    } break;
    case DATE: {
        templateInitializeVector<date_t>(result.get(), capacity);
    } break;
    case TIMESTAMP: {
        templateInitializeVector<timestamp_t>(result.get(), capacity);
    } break;
    case INTERVAL: {
        templateInitializeVector<interval_t>(result.get(), capacity);
    } break;
    case STRING: {
        templateInitializeVector<std::string>(result.get(), capacity);
    } break;
    default: {
        throw RuntimeException("Unsupported data type " + Types::dataTypeToString(type) +
                               " for arrow vector initialization.");
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

void ArrowRowBatch::appendValue(ArrowVector* vector, Value* value) {
    if (value->isNull_) {
        copyNullValue(vector, value, vector->numValues);
    } else {
        copyNonNullValue(vector, value, vector->numValues);
    }
    vector->numValues++;
}

template<typename T>
void ArrowRowBatch::templateCopyNonNullValue(ArrowVector* vector, Value* value, std::int64_t pos) {
    std::memcpy(vector->data.data() + pos * sizeof(T), &value->val, sizeof(T));
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<bool>(
    ArrowVector* vector, Value* value, std::int64_t pos) {
    if (value->val.booleanVal) {
        setBitToOne(vector->data.data(), pos);
    } else {
        setBitToZero(vector->data.data(), pos);
    }
}

template<>
void ArrowRowBatch::templateCopyNonNullValue<std::string>(
    ArrowVector* vector, Value* value, std::int64_t pos) {
    auto offsets = (std::uint32_t*)vector->data.data();
    auto strLength = value->strVal.length();
    offsets[pos + 1] = offsets[pos] + strLength;
    vector->overflow.resize(offsets[pos + 1]);
    std::memcpy(vector->overflow.data() + offsets[pos], value->strVal.data(), strLength);
}

void ArrowRowBatch::copyNonNullValue(ArrowVector* vector, Value* value, std::int64_t pos) {
    switch (value->dataType.typeID) {
    case BOOL: {
        templateCopyNonNullValue<bool>(vector, value, pos);
    } break;
    case INT64: {
        templateCopyNonNullValue<std::int64_t>(vector, value, pos);
    } break;
    case DOUBLE: {
        templateCopyNonNullValue<std::double_t>(vector, value, pos);
    } break;
    case DATE: {
        templateCopyNonNullValue<date_t>(vector, value, pos);
    } break;
    case TIMESTAMP: {
        templateCopyNonNullValue<timestamp_t>(vector, value, pos);
    } break;
    case INTERVAL: {
        templateCopyNonNullValue<interval_t>(vector, value, pos);
    } break;
    case STRING: {
        templateCopyNonNullValue<std::string>(vector, value, pos);
    } break;
    default: {
        throw RuntimeException("Data type " + Types::dataTypeToString(value->dataType) +
                               " is not supported for arrow exportation.");
    }
    }
}

template<typename T>
void ArrowRowBatch::templateCopyNullValue(ArrowVector* vector, Value* value, std::int64_t pos) {
    setBitToZero(vector->validity.data(), vector->numValues);
    vector->numNulls++;
}

template<>
void ArrowRowBatch::templateCopyNullValue<std::string>(
    ArrowVector* vector, Value* value, std::int64_t pos) {
    auto offsets = (std::uint32_t*)vector->data.data();
    offsets[pos + 1] = offsets[pos];
    setBitToZero(vector->validity.data(), vector->numValues);
    vector->numNulls++;
}

void ArrowRowBatch::copyNullValue(ArrowVector* vector, Value* value, std::int64_t pos) {
    switch (value->dataType.typeID) {
    case BOOL: {
        templateCopyNullValue<bool>(vector, value, pos);
    } break;
    case INT64: {
        templateCopyNullValue<std::int64_t>(vector, value, pos);
    } break;
    case DOUBLE: {
        templateCopyNullValue<std::double_t>(vector, value, pos);
    } break;
    case DATE: {
        templateCopyNullValue<date_t>(vector, value, pos);
    } break;
    case TIMESTAMP: {
        templateCopyNullValue<timestamp_t>(vector, value, pos);
    } break;
    case INTERVAL: {
        templateCopyNullValue<interval_t>(vector, value, pos);
    } break;
    case STRING: {
        templateCopyNullValue<std::string>(vector, value, pos);
    } break;
    default: {
        throw RuntimeException("Data type " + Types::dataTypeToString(value->dataType) +
                               " is not supported for arrow exportation.");
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

static unique_ptr<ArrowArray> createArrayFromVector(ArrowVector& vector) {
    auto result = make_unique<ArrowArray>();
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

template<typename T>
ArrowArray* ArrowRowBatch::templateCreateArray(ArrowVector& vector) {
    auto result = createArrayFromVector(vector);
    vector.array = std::move(result);
    return vector.array.get();
}

template<>
ArrowArray* ArrowRowBatch::templateCreateArray<string>(ArrowVector& vector) {
    auto result = createArrayFromVector(vector);
    result->n_buffers = 3;
    result->buffers[2] = vector.overflow.data();
    vector.array = std::move(result);
    return vector.array.get();
}

ArrowArray* ArrowRowBatch::convertVectorToArray(const DataType& type, ArrowVector& vector) {
    switch (type.typeID) {
    case BOOL: {
        return templateCreateArray<bool>(vector);
    }
    case INT64: {
        return templateCreateArray<std::int64_t>(vector);
    }
    case DOUBLE: {
        return templateCreateArray<std::double_t>(vector);
    }
    case DATE: {
        return templateCreateArray<date_t>(vector);
    }
    case TIMESTAMP: {
        return templateCreateArray<timestamp_t>(vector);
    }
    case INTERVAL: {
        return templateCreateArray<interval_t>(vector);
    }
    case STRING: {
        return templateCreateArray<string>(vector);
    }
    default: {
        throw RuntimeException("Data type " + Types::dataTypeToString(type) +
                               " is not supported for arrow exportation.");
    }
    }
}

ArrowArray ArrowRowBatch::toArray() {
    auto rootHolder = make_unique<ArrowVector>();
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
        rootHolder->childPointers[i] = convertVectorToArray(types[i], *rootHolder->childData[i]);
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
        vector<std::uint32_t> colWidths(numColumns, 10);
        for (auto i = 0u; i < numColumns; i++) {
            appendValue(vectors[i].get(), tuple->getValue(i));
        }
        numTuplesInBatch++;
    }
    numTuples += numTuplesInBatch;
    return toArray();
}
