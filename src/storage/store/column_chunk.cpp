#include "storage/store/column_chunk.h"

#include "common/types/value/nested.h"
#include "storage/storage_structure/storage_structure_utils.h"
#include "storage/store/compression.h"
#include "storage/store/string_column_chunk.h"
#include "storage/store/struct_column_chunk.h"
#include "storage/store/table_copy_utils.h"
#include "storage/store/var_list_column_chunk.h"

using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

ColumnChunkMetadata fixedSizedFlushBuffer(const uint8_t* buffer, uint64_t bufferSize,
    BMFileHandle* dataFH, page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) {
    FileUtils::writeToFile(dataFH->getFileInfo(), buffer, bufferSize,
        startPageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    return ColumnChunkMetadata(
        startPageIdx, metadata.numPages, metadata.numValues, metadata.compMeta);
}

ColumnChunkMetadata fixedSizedGetMetadata(
    const uint8_t* /*buffer*/, uint64_t bufferSize, uint64_t /*capacity*/, uint64_t numValues) {
    return ColumnChunkMetadata(INVALID_PAGE_IDX, ColumnChunk::getNumPagesForBytes(bufferSize),
        numValues, CompressionMetadata());
}

ColumnChunkMetadata booleanGetMetadata(
    const uint8_t* /*buffer*/, uint64_t bufferSize, uint64_t /*capacity*/, uint64_t numValues) {
    return ColumnChunkMetadata(INVALID_PAGE_IDX, ColumnChunk::getNumPagesForBytes(bufferSize),
        numValues, CompressionMetadata(CompressionType::BOOLEAN_BITPACKING));
}

class CompressedFlushBuffer {
    std::shared_ptr<CompressionAlg> alg;
    const LogicalType& dataType;

public:
    CompressedFlushBuffer(std::shared_ptr<CompressionAlg> alg, LogicalType& dataType)
        : alg{std::move(alg)}, dataType{dataType} {}

    CompressedFlushBuffer(const CompressedFlushBuffer& other) = default;

    ColumnChunkMetadata operator()(const uint8_t* buffer, uint64_t /*bufferSize*/,
        BMFileHandle* dataFH, page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) {
        auto valuesRemaining = metadata.numValues;
        const uint8_t* bufferStart = buffer;
        auto compressedBuffer = std::make_unique<uint8_t[]>(BufferPoolConstants::PAGE_4KB_SIZE);
        auto numPages = 0;
        auto numValuesPerPage =
            metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        do {
            auto compressedSize = alg->compressNextPage(bufferStart, valuesRemaining,
                compressedBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE, metadata.compMeta);
            // Avoid underflows (when data is compressed to nothing, numValuesPerPage may be
            // UINT64_MAX)
            if (numValuesPerPage > valuesRemaining) {
                valuesRemaining = 0;
            } else {
                valuesRemaining -= numValuesPerPage;
            }
            FileUtils::writeToFile(dataFH->getFileInfo(), compressedBuffer.get(), compressedSize,
                (startPageIdx + numPages) * BufferPoolConstants::PAGE_4KB_SIZE);
            numPages++;
        } while (valuesRemaining > 0);
        return ColumnChunkMetadata(
            startPageIdx, metadata.numPages, metadata.numValues, metadata.compMeta);
    }
};

class GetCompressionMetadata {
    std::shared_ptr<CompressionAlg> alg;
    const LogicalType& dataType;

public:
    GetCompressionMetadata(std::shared_ptr<CompressionAlg> alg, LogicalType& dataType)
        : alg{std::move(alg)}, dataType{dataType} {}

    GetCompressionMetadata(const GetCompressionMetadata& other) = default;

    ColumnChunkMetadata operator()(
        const uint8_t* buffer, uint64_t /*bufferSize*/, uint64_t capacity, uint64_t numValues) {
        auto metadata = alg->getCompressionMetadata(buffer, numValues);
        auto numValuesPerPage = metadata.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        auto numPages = capacity / numValuesPerPage + (capacity % numValuesPerPage == 0 ? 0 : 1);
        return ColumnChunkMetadata(INVALID_PAGE_IDX, numPages, numValues, metadata);
    }
};

static std::shared_ptr<CompressionAlg> getCompression(
    const LogicalType& dataType, bool enableCompression) {
    if (!enableCompression) {
        return std::make_shared<Uncompressed>(dataType);
    }
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::INT64: {
        return std::make_shared<IntegerBitpacking<int64_t>>();
    }
    case PhysicalTypeID::INT32: {
        return std::make_shared<IntegerBitpacking<int32_t>>();
    }
    case PhysicalTypeID::INT16: {
        return std::make_shared<IntegerBitpacking<int16_t>>();
    }
    case PhysicalTypeID::INT8: {
        return std::make_shared<IntegerBitpacking<int8_t>>();
    }
    case PhysicalTypeID::VAR_LIST:
    case PhysicalTypeID::UINT64: {
        return std::make_shared<IntegerBitpacking<uint64_t>>();
    }
    case PhysicalTypeID::UINT32: {
        return std::make_shared<IntegerBitpacking<uint32_t>>();
    }
    case PhysicalTypeID::UINT16: {
        return std::make_shared<IntegerBitpacking<uint16_t>>();
    }
    case PhysicalTypeID::UINT8: {
        return std::make_shared<IntegerBitpacking<uint8_t>>();
    }
    default: {
        return std::make_shared<Uncompressed>(dataType);
    }
    }
}

ColumnChunk::ColumnChunk(LogicalType dataType, bool enableCompression, bool hasNullChunk)
    : dataType{std::move(dataType)},
      numBytesPerValue{getDataTypeSizeInChunk(this->dataType)}, numValues{0} {
    if (hasNullChunk) {
        nullChunk = std::make_unique<NullColumnChunk>();
    }
    initializeBuffer(StorageConstants::NODE_GROUP_SIZE);
    initializeFunction(enableCompression);
}

void ColumnChunk::initializeBuffer(offset_t capacity_) {
    numBytesPerValue = getDataTypeSizeInChunk(dataType);
    capacity = capacity_;
    bufferSize = getBufferSize();
    buffer = std::make_unique<uint8_t[]>(bufferSize);
    if (nullChunk) {
        nullChunk->initializeBuffer(capacity_);
    }
}

void ColumnChunk::initializeFunction(bool enableCompression) {
    switch (this->dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        // Since we compress into memory, storage is the same as fixed-sized values,
        // but we need to mark it as being boolean compressed.
        flushBufferFunction = fixedSizedFlushBuffer;
        getMetadataFunction = booleanGetMetadata;
        break;
    }
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::INT8:
    case PhysicalTypeID::VAR_LIST:
    case PhysicalTypeID::UINT64:
    case PhysicalTypeID::UINT32:
    case PhysicalTypeID::UINT16:
    case PhysicalTypeID::UINT8:
    case PhysicalTypeID::INT128: {
        auto compression = getCompression(this->dataType, enableCompression);
        flushBufferFunction = CompressedFlushBuffer(compression, this->dataType);
        getMetadataFunction = GetCompressionMetadata(compression, this->dataType);
        break;
    }
    default: {
        flushBufferFunction = fixedSizedFlushBuffer;
        getMetadataFunction = fixedSizedGetMetadata;
    }
    }
}

void ColumnChunk::resetToEmpty() {
    if (nullChunk) {
        nullChunk->resetNullBuffer();
    }
    numValues = 0;
}

void ColumnChunk::append(ValueVector* vector, offset_t startPosInChunk) {
    copyVectorToBuffer(vector, startPosInChunk);
    numValues += vector->state->selVector->selectedSize;
}

void ColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    if (nullChunk) {
        nullChunk->append(
            other->nullChunk.get(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    }
    memcpy(buffer.get() + startPosInChunk * numBytesPerValue,
        other->buffer.get() + startPosInOtherChunk * numBytesPerValue,
        numValuesToAppend * numBytesPerValue);
    numValues += numValuesToAppend;
}

void ColumnChunk::write(const Value& val, uint64_t posToWrite) {
    nullChunk->setNull(posToWrite, val.isNull());
    if (val.isNull()) {
        return;
    }
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        setValue(val.getValue<bool>(), posToWrite);
    } break;
    case PhysicalTypeID::INT64: {
        setValue(val.getValue<int64_t>(), posToWrite);
    } break;
    case PhysicalTypeID::INT32: {
        setValue(val.getValue<int32_t>(), posToWrite);
    } break;
    case PhysicalTypeID::INT16: {
        setValue(val.getValue<int16_t>(), posToWrite);
    } break;
    case PhysicalTypeID::INT8: {
        setValue(val.getValue<int8_t>(), posToWrite);
    } break;
    case PhysicalTypeID::UINT64: {
        setValue(val.getValue<uint64_t>(), posToWrite);
    } break;
    case PhysicalTypeID::UINT32: {
        setValue(val.getValue<uint32_t>(), posToWrite);
    } break;
    case PhysicalTypeID::UINT16: {
        setValue(val.getValue<uint16_t>(), posToWrite);
    } break;
    case PhysicalTypeID::UINT8: {
        setValue(val.getValue<uint8_t>(), posToWrite);
    } break;
    case PhysicalTypeID::INT128: {
        setValue(val.getValue<int128_t>(), posToWrite);
    } break;
    case PhysicalTypeID::DOUBLE: {
        setValue(val.getValue<double_t>(), posToWrite);
    } break;
    case PhysicalTypeID::FLOAT: {
        setValue(val.getValue<float_t>(), posToWrite);
    } break;
    case PhysicalTypeID::INTERVAL: {
        setValue(val.getValue<interval_t>(), posToWrite);
    } break;
    default: {
        throw NotImplementedException{"ColumnChunk::write"};
    }
    }
}

void ColumnChunk::resize(uint64_t newCapacity) {
    if (numBytesPerValue != 0) {
        // Avoid resizing struct/serial columns.
        capacity = newCapacity;
        auto numBytesAfterResize = getBufferSize();
        assert(numBytesAfterResize > bufferSize);
        auto resizedBuffer = std::make_unique<uint8_t[]>(numBytesAfterResize);
        if (dataType.getPhysicalType() == common::PhysicalTypeID::BOOL) {
            memset(resizedBuffer.get(), 0 /* non null */, numBytesAfterResize);
        }
        memcpy(resizedBuffer.get(), buffer.get(), bufferSize);
        bufferSize = numBytesAfterResize;
        buffer = std::move(resizedBuffer);
    }
    if (nullChunk) {
        nullChunk->resize(newCapacity);
    }
}

void ColumnChunk::populateWithDefaultVal(ValueVector* defaultValueVector) {
    defaultValueVector->setState(std::make_shared<DataChunkState>());
    auto valPos = defaultValueVector->state->selVector->selectedPositions[0];
    defaultValueVector->state->selVector->resetSelectorToValuePosBufferWithSize(
        DEFAULT_VECTOR_CAPACITY);
    for (auto i = 0u; i < defaultValueVector->state->selVector->selectedSize; i++) {
        defaultValueVector->state->selVector->selectedPositions[i] = valPos;
    }
    auto numValuesAppended = 0u;
    while (numValuesAppended < StorageConstants::NODE_GROUP_SIZE) {
        auto numValuesToAppend = std::min(
            DEFAULT_VECTOR_CAPACITY, StorageConstants::NODE_GROUP_SIZE - numValuesAppended);
        defaultValueVector->state->selVector->selectedSize = numValuesToAppend;
        append(defaultValueVector, numValuesAppended);
        numValuesAppended += numValuesToAppend;
    }
}

offset_t ColumnChunk::getOffsetInBuffer(offset_t pos) const {
    auto numElementsInAPage =
        PageUtils::getNumElementsInAPage(numBytesPerValue, false /* hasNull */);
    auto posCursor = PageUtils::getPageByteCursorForPos(pos, numElementsInAPage, numBytesPerValue);
    auto offsetInBuffer =
        posCursor.pageIdx * BufferPoolConstants::PAGE_4KB_SIZE + posCursor.offsetInPage;
    assert(offsetInBuffer + numBytesPerValue <= bufferSize);
    return offsetInBuffer;
}

void ColumnChunk::copyVectorToBuffer(ValueVector* vector, offset_t startPosInChunk) {
    auto bufferToWrite = buffer.get() + startPosInChunk * numBytesPerValue;
    auto vectorDataToWriteFrom = vector->getData();
    if (vector->state->selVector->isUnfiltered()) {
        memcpy(bufferToWrite, vectorDataToWriteFrom,
            vector->state->selVector->selectedSize * numBytesPerValue);
        for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
            nullChunk->setNull(startPosInChunk + i, vector->isNull(i));
        }
    } else {
        for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
            auto pos = vector->state->selVector->selectedPositions[i];
            nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
            memcpy(bufferToWrite, vectorDataToWriteFrom + pos * numBytesPerValue, numBytesPerValue);
            bufferToWrite += numBytesPerValue;
        }
    }
}

void ColumnChunk::update(ValueVector* vector, vector_idx_t vectorIdx) {
    auto startOffsetInChunk = vectorIdx << DEFAULT_VECTOR_CAPACITY_LOG_2;
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        auto offsetInChunk = startOffsetInChunk + pos;
        nullChunk->setNull(offsetInChunk, vector->isNull(pos));
        if (!vector->isNull(pos)) {
            memcpy(buffer.get() + offsetInChunk * numBytesPerValue,
                vector->getData() + pos * numBytesPerValue, numBytesPerValue);
        }
        if (pos >= numValues) {
            numValues = pos + 1;
        }
    }
}

ColumnChunkMetadata ColumnChunk::getMetadataToFlush() const {
    return getMetadataFunction(buffer.get(), bufferSize, capacity, numValues);
}

ColumnChunkMetadata ColumnChunk::flushBuffer(
    BMFileHandle* dataFH, page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) {
    return flushBufferFunction(buffer.get(), bufferSize, dataFH, startPageIdx, metadata);
}

uint32_t ColumnChunk::getDataTypeSizeInChunk(LogicalType& dataType) {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::SERIAL:
    case LogicalTypeID::STRUCT: {
        return 0;
    }
    case LogicalTypeID::STRING: {
        return sizeof(ku_string_t);
    }
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::INTERNAL_ID: {
        return sizeof(offset_t);
    }
    // Used by NodeColumn to represent the de-compressed size of booleans in-memory
    // Does not reflect the size of booleans on-disk in BoolColumnChunk
    case LogicalTypeID::BOOL: {
        return 1;
    }
    default: {
        return StorageUtils::getDataTypeSize(dataType);
    }
    }
}

uint64_t ColumnChunk::getBufferSize() const {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        // 8 values per byte, and we need a buffer size which is a multiple of 8 bytes.
        return ceil(capacity / 8.0 / 8.0) * 8;
    }
    case LogicalTypeID::FIXED_LIST: {
        auto numElementsInAPage =
            PageUtils::getNumElementsInAPage(numBytesPerValue, false /* hasNull */);
        auto numPages = capacity / numElementsInAPage + (capacity % numElementsInAPage ? 1 : 0);
        return BufferPoolConstants::PAGE_4KB_SIZE * numPages;
    }
    default: {
        return numBytesPerValue * capacity;
    }
    }
}

void BoolColumnChunk::append(ValueVector* vector, offset_t startPosInChunk) {
    assert(vector->dataType.getPhysicalType() == PhysicalTypeID::BOOL);
    for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
        auto pos = vector->state->selVector->selectedPositions[i];
        nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
        NullMask::setNull(
            (uint64_t*)buffer.get(), startPosInChunk + i, vector->getValue<bool>(pos));
    }
    numValues += vector->state->selVector->selectedSize;
}

void BoolColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    NullMask::copyNullMask((uint64_t*)static_cast<BoolColumnChunk*>(other)->buffer.get(),
        startPosInOtherChunk, (uint64_t*)buffer.get(), startPosInChunk, numValuesToAppend);
    if (nullChunk) {
        nullChunk->append(
            other->getNullChunk(), startPosInOtherChunk, startPosInChunk, numValuesToAppend);
    }
    numValues += numValuesToAppend;
}

void NullColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    offset_t startPosInChunk, uint32_t numValuesToAppend) {
    copyFromBuffer((uint64_t*)static_cast<NullColumnChunk*>(other)->buffer.get(),
        startPosInOtherChunk, startPosInChunk, numValuesToAppend);
}

class FixedListColumnChunk : public ColumnChunk {
public:
    FixedListColumnChunk(LogicalType dataType, bool enableCompression)
        : ColumnChunk(std::move(dataType), enableCompression, true /* hasNullChunk */) {}

    void append(ColumnChunk* other, offset_t startPosInOtherChunk, offset_t startPosInChunk,
        uint32_t numValuesToAppend) final {
        auto otherChunk = (FixedListColumnChunk*)other;
        if (nullChunk) {
            nullChunk->append(otherChunk->nullChunk.get(), startPosInOtherChunk, startPosInChunk,
                numValuesToAppend);
        }
        // TODO(Guodong): This can be optimized to not copy one by one.
        for (auto i = 0u; i < numValuesToAppend; i++) {
            memcpy(buffer.get() + getOffsetInBuffer(startPosInChunk + i),
                otherChunk->buffer.get() + getOffsetInBuffer(startPosInOtherChunk + i),
                numBytesPerValue);
        }
        numValues += numValuesToAppend;
    }

    void write(const Value& fixedListVal, uint64_t posToWrite) final {
        assert(fixedListVal.getDataType()->getPhysicalType() == PhysicalTypeID::FIXED_LIST);
        nullChunk->setNull(posToWrite, fixedListVal.isNull());
        if (fixedListVal.isNull()) {
            return;
        }
        auto numValues = NestedVal::getChildrenSize(&fixedListVal);
        auto childType = FixedListType::getChildType(fixedListVal.getDataType());
        auto numBytesPerValueInList = getDataTypeSizeInChunk(*childType);
        auto bufferToWrite = buffer.get() + posToWrite * numBytesPerValue;
        for (auto i = 0u; i < numValues; i++) {
            auto val = NestedVal::getChildVal(&fixedListVal, i);
            switch (childType->getPhysicalType()) {
            case PhysicalTypeID::INT64: {
                memcpy(bufferToWrite, &val->getValueReference<int64_t>(), numBytesPerValueInList);
            } break;
            case PhysicalTypeID::INT32: {
                memcpy(bufferToWrite, &val->getValueReference<int32_t>(), numBytesPerValueInList);
            } break;
            case PhysicalTypeID::INT16: {
                memcpy(bufferToWrite, &val->getValueReference<int16_t>(), numBytesPerValueInList);
            } break;
            case PhysicalTypeID::DOUBLE: {
                memcpy(bufferToWrite, &val->getValueReference<double_t>(), numBytesPerValueInList);
            } break;
            case PhysicalTypeID::FLOAT: {
                memcpy(bufferToWrite, &val->getValueReference<float_t>(), numBytesPerValueInList);
            } break;
            default: {
                throw NotImplementedException{"FixedListColumnChunk::write"};
            }
            }
            bufferToWrite += numBytesPerValueInList;
        }
    }

    void copyVectorToBuffer(ValueVector* vector, offset_t startPosInChunk) final {
        auto vectorDataToWriteFrom = vector->getData();
        for (auto i = 0u; i < vector->state->selVector->selectedSize; i++) {
            auto pos = vector->state->selVector->selectedPositions[i];
            nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
            memcpy(buffer.get() + getOffsetInBuffer(startPosInChunk + i),
                vectorDataToWriteFrom + pos * numBytesPerValue, numBytesPerValue);
        }
    }
};

std::unique_ptr<ColumnChunk> ColumnChunkFactory::createColumnChunk(
    const LogicalType& dataType, bool enableCompression) {
    std::unique_ptr<ColumnChunk> chunk;
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        chunk = std::make_unique<BoolColumnChunk>();
    } break;
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::INT8:
    case PhysicalTypeID::UINT64:
    case PhysicalTypeID::UINT32:
    case PhysicalTypeID::UINT16:
    case PhysicalTypeID::UINT8:
    case PhysicalTypeID::INT128:
    case PhysicalTypeID::DOUBLE:
    case PhysicalTypeID::FLOAT:
    case PhysicalTypeID::INTERVAL: {
        if (dataType.getLogicalTypeID() == LogicalTypeID::SERIAL) {
            chunk = std::make_unique<ColumnChunk>(LogicalType(LogicalTypeID::SERIAL),
                false /*enableCompression*/, false /* hasNullChunk */);
        } else {
            chunk = std::make_unique<ColumnChunk>(dataType, enableCompression);
        }
    } break;
    case PhysicalTypeID::FIXED_LIST: {
        chunk = std::make_unique<FixedListColumnChunk>(dataType, enableCompression);
    } break;
    case PhysicalTypeID::STRING: {
        chunk = std::make_unique<StringColumnChunk>(dataType);
    } break;
    case PhysicalTypeID::VAR_LIST: {
        chunk = std::make_unique<VarListColumnChunk>(dataType, enableCompression);
    } break;
    case PhysicalTypeID::STRUCT: {
        chunk = std::make_unique<StructColumnChunk>(dataType, enableCompression);
    } break;
    default: {
        throw NotImplementedException("ColumnChunkFactory::createColumnChunk for data type " +
                                      LogicalTypeUtils::dataTypeToString(dataType) +
                                      " is not supported.");
    }
    }
    return chunk;
}

} // namespace storage
} // namespace kuzu
