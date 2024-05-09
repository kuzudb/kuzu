#include "storage/store/column_chunk.h"

#include "common/data_chunk/sel_vector.h"
#include "common/exception/copy.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "storage/compression/compression.h"
#include "storage/store/list_column_chunk.h"
#include "storage/store/string_column_chunk.h"
#include "storage/store/struct_column_chunk.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

ColumnChunkMetadata fixedSizedFlushBuffer(const uint8_t* buffer, uint64_t bufferSize,
    BMFileHandle* dataFH, page_idx_t startPageIdx, const ColumnChunkMetadata& metadata) {
    KU_ASSERT(dataFH->getNumPages() >= startPageIdx + metadata.numPages);
    dataFH->getFileInfo()->writeFile(buffer, bufferSize,
        startPageIdx * BufferPoolConstants::PAGE_4KB_SIZE);
    return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
        metadata.compMeta);
}

ColumnChunkMetadata fixedSizedGetMetadata(const uint8_t* /*buffer*/, uint64_t bufferSize,
    uint64_t /*capacity*/, uint64_t numValues) {
    return ColumnChunkMetadata(INVALID_PAGE_IDX, ColumnChunk::getNumPagesForBytes(bufferSize),
        numValues, CompressionMetadata());
}

ColumnChunkMetadata booleanGetMetadata(const uint8_t* /*buffer*/, uint64_t bufferSize,
    uint64_t /*capacity*/, uint64_t numValues) {
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
        auto numPages = 0u;
        auto numValuesPerPage =
            metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        KU_ASSERT(numValuesPerPage * metadata.numPages >= metadata.numValues);
        while (valuesRemaining > 0) {
            auto compressedSize = alg->compressNextPage(bufferStart, valuesRemaining,
                compressedBuffer.get(), BufferPoolConstants::PAGE_4KB_SIZE, metadata.compMeta);
            // Avoid underflows (when data is compressed to nothing, numValuesPerPage may be
            // UINT64_MAX)
            if (numValuesPerPage > valuesRemaining) {
                valuesRemaining = 0;
            } else {
                valuesRemaining -= numValuesPerPage;
            }
            if (compressedSize < BufferPoolConstants::PAGE_4KB_SIZE) {
                memset(compressedBuffer.get() + compressedSize, 0,
                    BufferPoolConstants::PAGE_4KB_SIZE - compressedSize);
            }
            KU_ASSERT(numPages < metadata.numPages);
            KU_ASSERT(dataFH->getNumPages() > startPageIdx + numPages);
            dataFH->getFileInfo()->writeFile(compressedBuffer.get(),
                BufferPoolConstants::PAGE_4KB_SIZE,
                (startPageIdx + numPages) * BufferPoolConstants::PAGE_4KB_SIZE);
            numPages++;
        }
        // Make sure that the file is the right length
        if (numPages < metadata.numPages) {
            memset(compressedBuffer.get(), 0, BufferPoolConstants::PAGE_4KB_SIZE);
            dataFH->getFileInfo()->writeFile(compressedBuffer.get(),
                BufferPoolConstants::PAGE_4KB_SIZE,
                (startPageIdx + metadata.numPages - 1) * BufferPoolConstants::PAGE_4KB_SIZE);
        }
        return ColumnChunkMetadata(startPageIdx, metadata.numPages, metadata.numValues,
            metadata.compMeta);
    }
};

class GetCompressionMetadata {
    std::shared_ptr<CompressionAlg> alg;
    const LogicalType& dataType;

public:
    GetCompressionMetadata(std::shared_ptr<CompressionAlg> alg, LogicalType& dataType)
        : alg{std::move(alg)}, dataType{dataType} {}

    GetCompressionMetadata(const GetCompressionMetadata& other) = default;

    ColumnChunkMetadata operator()(const uint8_t* buffer, uint64_t /*bufferSize*/,
        uint64_t capacity, uint64_t numValues) {
        auto metadata = alg->getCompressionMetadata(buffer, numValues);
        auto numValuesPerPage = metadata.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        auto numPages = capacity / numValuesPerPage + (capacity % numValuesPerPage == 0 ? 0 : 1);
        return ColumnChunkMetadata(INVALID_PAGE_IDX, numPages, numValues, metadata);
    }
};

static std::shared_ptr<CompressionAlg> getCompression(const LogicalType& dataType,
    bool enableCompression) {
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
    case PhysicalTypeID::INTERNAL_ID:
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST:
    case PhysicalTypeID::UINT64: {
        return std::make_shared<IntegerBitpacking<uint64_t>>();
    }
    case PhysicalTypeID::STRING:
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

ColumnChunk::ColumnChunk(LogicalType dataType, uint64_t capacity, bool enableCompression,
    bool hasNullChunk)
    : dataType{std::move(dataType)}, numBytesPerValue{getDataTypeSizeInChunk(this->dataType)},
      numValues{0}, enableCompression(enableCompression) {
    if (hasNullChunk) {
        nullChunk = std::make_unique<NullColumnChunk>(capacity, enableCompression);
    }
    initializeBuffer(capacity);
    initializeFunction();
}

void ColumnChunk::initializeBuffer(offset_t capacity_) {
    numBytesPerValue = getDataTypeSizeInChunk(dataType);
    capacity = capacity_;
    bufferSize = getBufferSize(capacity);
    buffer = std::make_unique<uint8_t[]>(bufferSize);
    if (nullChunk) {
        nullChunk->initializeBuffer(capacity_);
    }
}

void ColumnChunk::initializeFunction() {
    switch (this->dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        // Since we compress into memory, storage is the same as fixed-sized values,
        // but we need to mark it as being boolean compressed.
        flushBufferFunction = fixedSizedFlushBuffer;
        getMetadataFunction = booleanGetMetadata;
        break;
    }
    case PhysicalTypeID::STRING:
    case PhysicalTypeID::INT64:
    case PhysicalTypeID::INT32:
    case PhysicalTypeID::INT16:
    case PhysicalTypeID::INT8:
    case PhysicalTypeID::INTERNAL_ID:
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST:
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
        nullChunk->resetToEmpty();
    }
    KU_ASSERT(bufferSize == getBufferSize(capacity));
    memset(buffer.get(), 0x00, bufferSize);
    numValues = 0;
}

void ColumnChunk::append(ValueVector* vector, const SelectionVector& selVector) {
    KU_ASSERT(vector->dataType.getPhysicalType() == dataType.getPhysicalType());
    copyVectorToBuffer(vector, numValues, selVector);
    numValues += selVector.getSelSize();
}

void ColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    KU_ASSERT(other->dataType.getPhysicalType() == dataType.getPhysicalType());
    if (nullChunk) {
        KU_ASSERT(nullChunk->getNumValues() == getNumValues());
        nullChunk->append(other->nullChunk.get(), startPosInOtherChunk, numValuesToAppend);
    }
    KU_ASSERT(numValues + numValuesToAppend <= capacity);
    memcpy(buffer.get() + numValues * numBytesPerValue,
        other->buffer.get() + startPosInOtherChunk * numBytesPerValue,
        numValuesToAppend * numBytesPerValue);
    numValues += numValuesToAppend;
}

void ColumnChunk::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < capacity);
    output.setNull(posInOutputVector, nullChunk->isNull(offsetInChunk));
    if (!output.isNull(posInOutputVector)) {
        memcpy(output.getData() + posInOutputVector * numBytesPerValue,
            buffer.get() + offsetInChunk * numBytesPerValue, numBytesPerValue);
    }
}

void ColumnChunk::write(ColumnChunk* chunk, ColumnChunk* dstOffsets, RelMultiplicity multiplicity) {
    KU_ASSERT(chunk->dataType.getPhysicalType() == dataType.getPhysicalType() &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID &&
              chunk->getNumValues() == dstOffsets->getNumValues());
    for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
        auto dstOffset = dstOffsets->getValue<offset_t>(i);
        KU_ASSERT(dstOffset < capacity);
        if (multiplicity == RelMultiplicity::ONE && !nullChunk->isNull(dstOffset)) {
            throw CopyException(stringFormat("Node with offset: {} can only have one neighbour due "
                                             "to the MANY-ONE/ONE-ONE relationship constraint.",
                dstOffset));
        }
        if (!chunk->getNullChunk()->isNull(i)) {
            memcpy(buffer.get() + dstOffset * numBytesPerValue,
                chunk->getData() + i * numBytesPerValue, numBytesPerValue);
        }
        nullChunk->setNull(dstOffset, chunk->getNullChunk()->isNull(i));
        numValues = dstOffset >= numValues ? dstOffset + 1 : numValues;
    }
}

// NOTE: This function is only called in LocalTable right now when performing out-of-place
// committing. LIST has a different logic for handling out-of-place committing as it has to
// be slided. However, this is unsafe, as this function can also be used for other purposes later.
// Thus, an assertion is added at the first line.
void ColumnChunk::write(ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    KU_ASSERT(dataType.getPhysicalType() != PhysicalTypeID::BOOL &&
              dataType.getPhysicalType() != PhysicalTypeID::LIST &&
              dataType.getPhysicalType() != PhysicalTypeID::ARRAY);
    nullChunk->setNull(offsetInChunk, vector->isNull(offsetInVector));
    if (offsetInChunk >= numValues) {
        numValues = offsetInChunk + 1;
    }
    if (!vector->isNull(offsetInVector)) {
        memcpy(buffer.get() + offsetInChunk * numBytesPerValue,
            vector->getData() + offsetInVector * numBytesPerValue, numBytesPerValue);
    }
}

void ColumnChunk::write(ColumnChunk* srcChunk, offset_t srcOffsetInChunk, offset_t dstOffsetInChunk,
    offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->dataType.getPhysicalType() == dataType.getPhysicalType());
    if ((dstOffsetInChunk + numValuesToCopy) >= numValues) {
        numValues = dstOffsetInChunk + numValuesToCopy;
    }
    memcpy(buffer.get() + dstOffsetInChunk * numBytesPerValue,
        srcChunk->buffer.get() + srcOffsetInChunk * numBytesPerValue,
        numValuesToCopy * numBytesPerValue);
    nullChunk->write(srcChunk->getNullChunk(), srcOffsetInChunk, dstOffsetInChunk, numValuesToCopy);
}

void ColumnChunk::copy(ColumnChunk* srcChunk, offset_t srcOffsetInChunk, offset_t dstOffsetInChunk,
    offset_t numValuesToCopy) {
    KU_ASSERT(srcChunk->dataType.getPhysicalType() == dataType.getPhysicalType());
    KU_ASSERT(dstOffsetInChunk >= numValues);
    while (numValues < dstOffsetInChunk) {
        nullChunk->setNull(numValues, true);
        numValues++;
    }
    append(srcChunk, srcOffsetInChunk, numValuesToCopy);
}

void ColumnChunk::resize(uint64_t newCapacity) {
    if (newCapacity > capacity) {
        capacity = newCapacity;
    }
    auto numBytesAfterResize = getBufferSize(newCapacity);
    if (numBytesAfterResize > bufferSize) {
        auto resizedBuffer = std::make_unique<uint8_t[]>(numBytesAfterResize);
        memcpy(resizedBuffer.get(), buffer.get(), bufferSize);
        bufferSize = numBytesAfterResize;
        buffer = std::move(resizedBuffer);
    }
    if (nullChunk) {
        nullChunk->resize(newCapacity);
    }
}

void ColumnChunk::populateWithDefaultVal(ValueVector* defaultValueVector) {
    // TODO(Guodong): don't set vector state to a new one. Default vector is shared across all
    // operators on the pipeline so setting its state will affect others.
    // You can only set state for vectors that is local to this class.
    defaultValueVector->setState(std::make_shared<DataChunkState>());
    auto valPos = defaultValueVector->state->getSelVector()[0];
    auto positionBuffer = defaultValueVector->state->getSelVectorUnsafe().getMultableBuffer();
    for (auto i = 0u; i < defaultValueVector->state->getSelVector().getSelSize(); i++) {
        positionBuffer[i] = valPos;
    }
    defaultValueVector->state->getSelVectorUnsafe().setToFiltered(DEFAULT_VECTOR_CAPACITY);
    auto numValuesAppended = 0u;
    auto numValuesToPopulate = capacity;
    while (numValuesAppended < numValuesToPopulate) {
        auto numValuesToAppend =
            std::min(DEFAULT_VECTOR_CAPACITY, numValuesToPopulate - numValuesAppended);
        defaultValueVector->state->getSelVectorUnsafe().setSelSize(numValuesToAppend);
        append(defaultValueVector, defaultValueVector->state->getSelVector());
        numValuesAppended += numValuesToAppend;
    }
}

void ColumnChunk::copyVectorToBuffer(ValueVector* vector, offset_t startPosInChunk,
    const SelectionVector& selVector) {
    auto bufferToWrite = buffer.get() + startPosInChunk * numBytesPerValue;
    KU_ASSERT(startPosInChunk + selVector.getSelSize() <= capacity);
    auto vectorDataToWriteFrom = vector->getData();
    if (selVector.isUnfiltered()) {
        memcpy(bufferToWrite, vectorDataToWriteFrom, selVector.getSelSize() * numBytesPerValue);
        // TODO(Guodong): Should be wrapped into nullChunk->append(vector);
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            nullChunk->setNull(startPosInChunk + i, vector->isNull(i));
        }
    } else {
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            auto pos = selVector[i];
            // TODO(Guodong): Should be wrapped into nullChunk->append(vector);
            nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
            memcpy(bufferToWrite, vectorDataToWriteFrom + pos * numBytesPerValue, numBytesPerValue);
            bufferToWrite += numBytesPerValue;
        }
    }
}

void ColumnChunk::setNumValues(uint64_t numValues_) {
    KU_ASSERT(numValues_ <= capacity);
    numValues = numValues_;
    if (nullChunk) {
        nullChunk->setNumValues(numValues_);
    }
}

bool ColumnChunk::numValuesSanityCheck() const {
    if (nullChunk) {
        return numValues == nullChunk->getNumValues();
    }
    return numValues <= capacity;
}

bool ColumnChunk::sanityCheck() {
    if (nullChunk) {
        return nullChunk->sanityCheck() && numValuesSanityCheck();
    }
    return numValues <= capacity;
}

ColumnChunkMetadata ColumnChunk::getMetadataToFlush() const {
    KU_ASSERT(numValues <= capacity);
    if (enableCompression) {
        // Determine if we can make use of constant compression
        auto constantMetadata = ConstantCompression::analyze(*this);
        if (constantMetadata) {
            return ColumnChunkMetadata(INVALID_PAGE_IDX, 0, numValues, *constantMetadata);
        }
    }
    KU_ASSERT(bufferSize == getBufferSize(capacity));
    return getMetadataFunction(buffer.get(), bufferSize, capacity, numValues);
}

ColumnChunkMetadata ColumnChunk::flushBuffer(BMFileHandle* dataFH, page_idx_t startPageIdx,
    const ColumnChunkMetadata& metadata) {
    if (!metadata.compMeta.isConstant()) {
        KU_ASSERT(bufferSize == getBufferSize(capacity));
        return flushBufferFunction(buffer.get(), bufferSize, dataFH, startPageIdx, metadata);
    }
    return metadata;
}

uint64_t ColumnChunk::getBufferSize(uint64_t capacity_) const {
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::BOOL: {
        // 8 values per byte, and we need a buffer size which is a multiple of 8 bytes.
        return ceil(capacity_ / 8.0 / 8.0) * 8;
    }
    default: {
        return numBytesPerValue * capacity_;
    }
    }
}

void BoolColumnChunk::append(ValueVector* vector, const SelectionVector& selVector) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::BOOL);
    for (auto i = 0u; i < selVector.getSelSize(); i++) {
        auto pos = selVector[i];
        nullChunk->setNull(numValues + i, vector->isNull(pos));
        NullMask::setNull((uint64_t*)buffer.get(), numValues + i, vector->getValue<bool>(pos));
    }
    numValues += selVector.getSelSize();
}

void BoolColumnChunk::append(ColumnChunk* other, offset_t startPosInOtherChunk,
    uint32_t numValuesToAppend) {
    NullMask::copyNullMask((uint64_t*)static_cast<BoolColumnChunk*>(other)->buffer.get(),
        startPosInOtherChunk, (uint64_t*)buffer.get(), numValues, numValuesToAppend);
    if (nullChunk) {
        nullChunk->append(other->getNullChunk(), startPosInOtherChunk, numValuesToAppend);
    }
    numValues += numValuesToAppend;
}

void BoolColumnChunk::lookup(offset_t offsetInChunk, ValueVector& output,
    sel_t posInOutputVector) const {
    KU_ASSERT(offsetInChunk < capacity);
    output.setNull(posInOutputVector, nullChunk->isNull(offsetInChunk));
    if (!output.isNull(posInOutputVector)) {
        output.setValue<bool>(posInOutputVector,
            NullMask::isNull((uint64_t*)buffer.get(), offsetInChunk));
    }
}

void BoolColumnChunk::write(ColumnChunk* chunk, ColumnChunk* dstOffsets,
    RelMultiplicity /*multiplicity*/) {
    KU_ASSERT(chunk->getDataType().getPhysicalType() == PhysicalTypeID::BOOL &&
              dstOffsets->getDataType().getPhysicalType() == PhysicalTypeID::INTERNAL_ID &&
              chunk->getNumValues() == dstOffsets->getNumValues());
    for (auto i = 0u; i < dstOffsets->getNumValues(); i++) {
        auto dstOffset = dstOffsets->getValue<offset_t>(i);
        KU_ASSERT(dstOffset < capacity);
        NullMask::setNull((uint64_t*)buffer.get(), dstOffset, chunk->getValue<bool>(i));
        if (nullChunk) {
            nullChunk->setNull(dstOffset, chunk->getNullChunk()->isNull(i));
        }
        numValues = dstOffset >= numValues ? dstOffset + 1 : numValues;
    }
}

void BoolColumnChunk::write(ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::BOOL);
    KU_ASSERT(offsetInChunk < capacity);
    setValue(vector->getValue<bool>(offsetInVector), offsetInChunk);
    if (nullChunk) {
        nullChunk->write(vector, offsetInVector, offsetInChunk);
    }
    numValues = offsetInChunk >= numValues ? offsetInChunk + 1 : numValues;
}

void BoolColumnChunk::write(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    if (nullChunk) {
        nullChunk->write(srcChunk->getNullChunk(), srcOffsetInChunk, dstOffsetInChunk,
            numValuesToCopy);
    }
    if ((dstOffsetInChunk + numValuesToCopy) >= numValues) {
        numValues = dstOffsetInChunk + numValuesToCopy;
    }
    NullMask::copyNullMask((uint64_t*)static_cast<BoolColumnChunk*>(srcChunk)->buffer.get(),
        srcOffsetInChunk, (uint64_t*)buffer.get(), dstOffsetInChunk, numValuesToCopy);
}

void NullColumnChunk::setNull(offset_t pos, bool isNull) {
    setValue(isNull, pos);
    if (isNull) {
        mayHaveNullValue = true;
    }
    // TODO(Guodong): Better let NullColumnChunk also support `append` a vector.
    if (pos >= numValues) {
        numValues = pos + 1;
        KU_ASSERT(numValues <= capacity);
    }
}

void NullColumnChunk::write(ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) {
    setNull(offsetInChunk, vector->isNull(offsetInVector));
    numValues = offsetInChunk >= numValues ? offsetInChunk + 1 : numValues;
}

void NullColumnChunk::write(ColumnChunk* srcChunk, offset_t srcOffsetInChunk,
    offset_t dstOffsetInChunk, offset_t numValuesToCopy) {
    if ((dstOffsetInChunk + numValuesToCopy) >= numValues) {
        numValues = dstOffsetInChunk + numValuesToCopy;
    }
    copyFromBuffer((uint64_t*)static_cast<NullColumnChunk*>(srcChunk)->buffer.get(),
        srcOffsetInChunk, dstOffsetInChunk, numValuesToCopy);
}

void NullColumnChunk::append(ColumnChunk* other, offset_t startOffsetInOtherChunk,
    uint32_t numValuesToAppend) {
    copyFromBuffer((uint64_t*)static_cast<NullColumnChunk*>(other)->buffer.get(),
        startOffsetInOtherChunk, numValues, numValuesToAppend);
    numValues += numValuesToAppend;
}

class InternalIDColumnChunk final : public ColumnChunk {
public:
    // Physically, we only materialize offset of INTERNAL_ID, which is same as UINT64,
    explicit InternalIDColumnChunk(uint64_t capacity, bool enableCompression)
        : ColumnChunk(*LogicalType::INTERNAL_ID(), capacity, enableCompression),
          commonTableID{INVALID_TABLE_ID} {}

    void append(ValueVector* vector, const common::SelectionVector& selVector) override {
        switch (vector->dataType.getPhysicalType()) {
        case PhysicalTypeID::INTERNAL_ID: {
            copyVectorToBuffer(vector, numValues, selVector);
        } break;
        case PhysicalTypeID::INT64: {
            copyInt64VectorToBuffer(vector, numValues, selVector);
        } break;
        default: {
            KU_UNREACHABLE;
        }
        }
        numValues += selVector.getSelSize();
    }

    void copyVectorToBuffer(ValueVector* vector, offset_t startPosInChunk,
        const common::SelectionVector& selVector) override {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
        auto relIDsInVector = (internalID_t*)vector->getData();
        if (commonTableID == INVALID_TABLE_ID) {
            commonTableID = relIDsInVector[selVector[0]].tableID;
        }
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            auto pos = selVector[i];
            KU_ASSERT(relIDsInVector[pos].tableID == commonTableID);
            nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
            memcpy(buffer.get() + (startPosInChunk + i) * numBytesPerValue,
                &relIDsInVector[pos].offset, numBytesPerValue);
        }
    }

    void copyInt64VectorToBuffer(ValueVector* vector, offset_t startPosInChunk,
        const common::SelectionVector& selVector) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INT64);
        for (auto i = 0u; i < selVector.getSelSize(); i++) {
            auto pos = selVector[i];
            nullChunk->setNull(startPosInChunk + i, vector->isNull(pos));
            memcpy(buffer.get() + (startPosInChunk + i) * numBytesPerValue,
                &vector->getValue<offset_t>(pos), numBytesPerValue);
        }
    }

    void lookup(offset_t offsetInChunk, ValueVector& output,
        sel_t posInOutputVector) const override {
        KU_ASSERT(offsetInChunk < capacity);
        output.setNull(posInOutputVector, nullChunk->isNull(offsetInChunk));
        if (!output.isNull(posInOutputVector)) {
            auto relID = output.getValue<internalID_t>(posInOutputVector);
            relID.offset = getValue<offset_t>(offsetInChunk);
            KU_ASSERT(commonTableID != INVALID_TABLE_ID);
            relID.tableID = commonTableID;
            output.setValue<internalID_t>(posInOutputVector, relID);
        }
    }

    void write(ValueVector* vector, offset_t offsetInVector, offset_t offsetInChunk) override {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
        nullChunk->setNull(offsetInChunk, vector->isNull(offsetInVector));
        auto relIDsInVector = (internalID_t*)vector->getData();
        if (commonTableID == INVALID_TABLE_ID) {
            commonTableID = relIDsInVector[offsetInVector].tableID;
        }
        KU_ASSERT(commonTableID == relIDsInVector[offsetInVector].tableID);
        if (!vector->isNull(offsetInVector)) {
            memcpy(buffer.get() + offsetInChunk * numBytesPerValue,
                &relIDsInVector[offsetInVector].offset, numBytesPerValue);
        }
        if (offsetInChunk >= numValues) {
            numValues = offsetInChunk + 1;
        }
    }

private:
    common::table_id_t commonTableID;
};

std::unique_ptr<ColumnChunk> ColumnChunkFactory::createColumnChunk(LogicalType dataType,
    bool enableCompression, uint64_t capacity, bool inMemory) {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::BOOL: {
        return std::make_unique<BoolColumnChunk>(capacity, enableCompression);
    }
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
        // TODO: As we have constant compression, SERIAL column should always be compressed as
        // constant non-null when flushed to disk. We should add a sanity check for this.
        return std::make_unique<ColumnChunk>(std::move(dataType), capacity, enableCompression);
    }
        // Physically, we only materialize offset of INTERNAL_ID, which is same as INT64,
    case PhysicalTypeID::INTERNAL_ID: {
        return std::make_unique<InternalIDColumnChunk>(capacity, enableCompression);
    }
    case PhysicalTypeID::STRING: {
        return std::make_unique<StringColumnChunk>(std::move(dataType), capacity, enableCompression,
            inMemory);
    }
    case PhysicalTypeID::ARRAY:
    case PhysicalTypeID::LIST: {
        return std::make_unique<ListColumnChunk>(std::move(dataType), capacity, enableCompression,
            inMemory);
    }
    case PhysicalTypeID::STRUCT: {
        return std::make_unique<StructColumnChunk>(std::move(dataType), capacity, enableCompression,
            inMemory);
    }
    default:
        KU_UNREACHABLE;
    }
}

} // namespace storage
} // namespace kuzu
