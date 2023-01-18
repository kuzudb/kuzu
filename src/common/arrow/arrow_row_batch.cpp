#include "common/arrow/arrow_row_batch.h"

ArrowRowBatch::ArrowRowBatch(std::vector<DataType> types, std::int64_t capacity)
    : types{std::move(types)} {
    auto numVectors = this->types.size();
    vectors.resize(numVectors);
    for (auto i = 0u; i < numVectors; i++) {
        vectors[i] = make_unique<ArrowVector>();
        initializeArrowVector(vectors[i].get(), this->types[i], capacity);
    }
}

void ArrowRowBatch::initializeArrowVector(
    ArrowVector* vector, const DataType& type, std::int64_t capacity) {
    auto numBytesForValidity = getNumBytesForBits(capacity);
    vector->validity.resize(numBytesForValidity, 0xFF);
    std::int64_t numBytesForData = 0;
    if (type.typeID == BOOL) {
        numBytesForData = getNumBytesForBits(capacity);
    } else {
        numBytesForData = Types::getDataTypeSize(type) * capacity;
    }
    vector->data.reserve(numBytesForData);
}

static void getBitPosition(std::int64_t pos, std::int64_t& bytePos, std::int64_t& bitOffset) {
    bytePos = pos / 8;
    bitOffset = pos % 8;
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

void ArrowRowBatch::setValue(ArrowVector* vector, Value* value, std::int64_t pos) {
    if (value->isNull_) {
        setBitToZero(vector->validity.data(), pos);
        vector->numNulls++;
        return;
    }
    switch (value->dataType.typeID) {
    case BOOL: {
        if (value->val.booleanVal) {
            setBitToOne(vector->data.data(), pos);
        } else {
            setBitToZero(vector->data.data(), pos);
        }
    } break;
    case INT64: {
        ((std::int64_t*)vector->data.data())[pos] = value->val.int64Val;
    } break;
    case DOUBLE: {
        ((std::double_t*)vector->data.data())[pos] = value->val.doubleVal;
    } break;
    case DATE: {
        ((date_t*)vector->data.data())[pos] = value->val.dateVal;
    } break;
    case TIMESTAMP: {
        ((timestamp_t*)vector->data.data())[pos] = value->val.timestampVal;
    } break;
    case INTERVAL: {
        ((interval_t*)vector->data.data())[pos] = value->val.intervalVal;
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

ArrowArray* ArrowRowBatch::finalizeArrowChild(const DataType& type, ArrowVector& vector) {
    auto result = make_unique<ArrowArray>();

    result->private_data = nullptr;
    result->release = releaseArrowVector;
    result->n_children = 0;
    result->offset = 0;
    result->dictionary = nullptr;
    result->buffers = vector.buffers.data();
    result->null_count = vector.numNulls;
    result->length = vector.numValues;
    result->buffers[0] = vector.validity.data();
    result->buffers[1] = vector.data.data();
    result->n_buffers = 2;

    vector.array = std::move(result);
    return vector.array.get();
}

ArrowArray ArrowRowBatch::finalize() {
    auto rootHolder = make_unique<ArrowVector>();
    ArrowArray result;
    rootHolder->childPointers.resize(types.size());
    result.children = rootHolder->childPointers.data();
    result.n_children = (std::int64_t)types.size();
    result.length = numRows;
    result.n_buffers = 1;
    result.buffers = rootHolder->buffers.data(); // no actual buffer
    result.offset = 0;
    result.null_count = 0;
    result.dictionary = nullptr;
    rootHolder->childData = std::move(vectors);
    for (auto i = 0u; i < rootHolder->childData.size(); i++) {
        rootHolder->childPointers[i] = finalizeArrowChild(types[i], *rootHolder->childData[i]);
    }

    result.private_data = rootHolder.release();
    result.release = releaseArrowVector;
    return result;
}

ArrowArray ArrowRowBatch::append(main::QueryResult& queryResult, std::int64_t chunkSize) {
    std::int64_t numTuples = 0;
    auto numColumns = queryResult.getColumnNames().size();
    while (numTuples < chunkSize) {
        if (!queryResult.hasNext()) {
            break;
        }
        auto tuple = queryResult.getNext();
        for (auto i = 0u; i < numColumns; i++) {
            setValue(vectors[i].get(), tuple->getValue(i), numTuples);
        }
        numTuples++;
    }
    for (auto i = 0u; i < numColumns; i++) {
        vectors[i]->numValues = numTuples;
    }
    numRows = numTuples;
    return finalize();
}
