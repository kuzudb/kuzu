#include "storage/store/column.h"

#include <memory>

#include "common/assert.h"
#include "storage/stats/property_statistics.h"
#include "storage/store/column_chunk.h"
#include "storage/store/string_column.h"
#include "storage/store/struct_column.h"
#include "storage/store/var_list_column.h"
#include "transaction/transaction.h"
#include <bit>

using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace storage {

static inline bool isPageIdxValid(page_idx_t pageIdx, const ColumnChunkMetadata& metadata) {
    return (metadata.pageIdx <= pageIdx && pageIdx < metadata.pageIdx + metadata.numPages) ||
           (pageIdx == INVALID_PAGE_IDX && metadata.compMeta.isConstant());
}

struct ReadInternalIDValuesToVector {
    ReadInternalIDValuesToVector() : compressedReader{LogicalType(LogicalTypeID::INTERNAL_ID)} {}
    void operator()(const uint8_t* frame, PageElementCursor& pageCursor, ValueVector* resultVector,
        uint32_t posInVector, uint32_t numValuesToRead, const CompressionMetadata& metadata) {
        KU_ASSERT(resultVector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);

        std::unique_ptr<offset_t[]> buffer = std::make_unique<offset_t[]>(numValuesToRead);
        compressedReader(frame, pageCursor, (uint8_t*)buffer.get(), 0, numValuesToRead, metadata);
        auto resultData = (internalID_t*)resultVector->getData();
        for (auto i = 0u; i < numValuesToRead; i++) {
            resultData[posInVector + i].offset = buffer[i];
        }
    }

private:
    ReadCompressedValuesFromPage compressedReader;
};

struct WriteInternalIDValuesToPage {
    WriteInternalIDValuesToPage() : compressedWriter{LogicalType(LogicalTypeID::INTERNAL_ID)} {}
    void operator()(uint8_t* frame, uint16_t posInFrame, const uint8_t* data, uint32_t dataOffset,
        offset_t numValues, const CompressionMetadata& metadata) {
        auto internalIDValues = reinterpret_cast<const internalID_t*>(data);

        for (auto i = 0u; i < numValues; i++) {
            compressedWriter(frame, posInFrame,
                reinterpret_cast<const uint8_t*>(&internalIDValues[dataOffset + i].offset),
                0 /*dataOffset*/, 1 /*numValues*/, metadata);
        }
    }
    void operator()(uint8_t* frame, uint16_t posInFrame, ValueVector* vector,
        uint32_t offsetInVector, const CompressionMetadata& metadata) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
        compressedWriter(
            frame, posInFrame, vector->getData(), offsetInVector, 1 /*numValues*/, metadata);
    }

private:
    WriteCompressedValuesToPage compressedWriter;
};

struct NullColumnFunc {
    static void readValuesFromPageToVector(const uint8_t* frame, PageElementCursor& pageCursor,
        ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead,
        const CompressionMetadata& metadata) {
        // Read bit-packed null flags from the frame into the result vector
        // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
        // Otherwise, it could read off the end of the page.
        if (metadata.isConstant()) {
            auto value = ConstantCompression::getValue<bool>(metadata);
            resultVector->setNullRange(posInVector, numValuesToRead, value);
        } else {
            resultVector->setNullFromBits(
                (uint64_t*)frame, pageCursor.elemPosInPage, posInVector, numValuesToRead);
        }
    }

    static void writeValueToPageFromVector(uint8_t* frame, uint16_t posInFrame, ValueVector* vector,
        uint32_t posInVector, const CompressionMetadata& metadata) {
        if (metadata.isConstant()) {
            // Value to write is identical to the constant value
            return;
        }
        // Casting to uint64_t should be safe as long as the page size is a multiple of 8 bytes.
        // Otherwise, it could read off the end of the page.
        NullMask::setNull((uint64_t*)frame, posInFrame, vector->isNull(posInVector));
    }
};

static read_values_to_vector_func_t getReadValuesToVectorFunc(const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::INTERNAL_ID:
        return ReadInternalIDValuesToVector();
    default:
        return ReadCompressedValuesFromPageToVector(logicalType);
    }
}

static write_values_from_vector_func_t getWriteValueFromVectorFunc(const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::INTERNAL_ID:
        return WriteInternalIDValuesToPage();
    default:
        return WriteCompressedValuesToPage(logicalType);
    }
}

static write_values_func_t getWriteValuesFunc(const LogicalType& logicalType) {
    switch (logicalType.getLogicalTypeID()) {
    case LogicalTypeID::INTERNAL_ID:
        return WriteInternalIDValuesToPage();
    default:
        return WriteCompressedValuesToPage(logicalType);
    }
}

class NullColumn final : public Column {
    friend StructColumn;

public:
    // Page size must be aligned to 8 byte chunks for the 64-bit NullMask algorithms to work
    // without the possibility of memory errors from reading/writing off the end of a page.
    static_assert(PageUtils::getNumElementsInAPage(1, false /*requireNullColumn*/) % 8 == 0);

    NullColumn(std::string name, page_idx_t metaDAHPageIdx, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction,
        RWPropertyStats propertyStatistics, bool enableCompression)
        : Column{name, LogicalType::BOOL(), MetadataDAHInfo{metaDAHPageIdx}, dataFH, metadataFH,
              bufferManager, wal, transaction, propertyStatistics, enableCompression,
              false /*requireNullColumn*/} {
        readToVectorFunc = NullColumnFunc::readValuesFromPageToVector;
        writeFromVectorFunc = NullColumnFunc::writeValueToPageFromVector;
        // Should never be used
        batchLookupFunc = nullptr;
    }

    void scan(
        Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) override {
        if (propertyStatistics.mayHaveNull(*transaction)) {
            scanInternal(transaction, nodeIDVector, resultVector);
        } else {
            resultVector->setAllNonNull();
        }
    }

    void scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
        offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
        uint64_t offsetInVector) override {
        if (propertyStatistics.mayHaveNull(*transaction)) {
            Column::scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup,
                resultVector, offsetInVector);
        } else {
            resultVector->setNullRange(
                offsetInVector, endOffsetInGroup - startOffsetInGroup, false /*set non-null*/);
        }
    }

    void scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk) override {
        if (propertyStatistics.mayHaveNull(DUMMY_WRITE_TRANSACTION)) {
            Column::scan(transaction, nodeGroupIdx, columnChunk);
        } else {
            static_cast<NullColumnChunk*>(columnChunk)->resetToNoNull();
            if (nodeGroupIdx >= metadataDA->getNumElements(transaction->getType())) {
                columnChunk->setNumValues(0);
            } else {
                auto chunkMetadata = metadataDA->get(nodeGroupIdx, transaction->getType());
                columnChunk->setNumValues(chunkMetadata.numValues);
            }
        }
    }

    void lookup(
        Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) override {
        if (propertyStatistics.mayHaveNull(*transaction)) {
            lookupInternal(transaction, nodeIDVector, resultVector);
        } else {
            for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
                auto pos = nodeIDVector->state->selVector->selectedPositions[i];
                resultVector->setNull(pos, false);
            }
        }
    }

    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) override {
        auto preScanMetadata = columnChunk->getMetadataToFlush();
        auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);
        auto metadata = columnChunk->flushBuffer(dataFH, startPageIdx, preScanMetadata);
        metadataDA->resize(nodeGroupIdx + 1);
        metadataDA->update(nodeGroupIdx, metadata);
        if (static_cast<NullColumnChunk*>(columnChunk)->mayHaveNull()) {
            propertyStatistics.setHasNull(DUMMY_WRITE_TRANSACTION);
        }
    }

    bool isNull(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
        offset_t offsetInChunk) override {
        auto state = getReadState(transaction->getType(), nodeGroupIdx);
        uint64_t result = false;
        if (offsetInChunk >= state.metadata.numValues) {
            return true;
        }
        // Must be aligned to an 8-byte chunk for NullMask read to not overflow
        Column::scan(transaction, state, offsetInChunk, offsetInChunk + 1,
            reinterpret_cast<uint8_t*>(&result));
        return result;
    }

    void setNull(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk) override {
        auto chunkMeta = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
        propertyStatistics.setHasNull(DUMMY_WRITE_TRANSACTION);
        // Must be aligned to an 8-byte chunk for NullMask read to not overflow
        uint64_t value = true;
        writeValue(
            chunkMeta, nodeGroupIdx, offsetInChunk, reinterpret_cast<const uint8_t*>(&value));
        if (offsetInChunk >= chunkMeta.numValues) {
            chunkMeta.numValues = offsetInChunk + 1;
            metadataDA->update(nodeGroupIdx, chunkMeta);
        }
    }

    void write(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk,
        ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override {
        auto chunkMeta = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
        writeValue(
            chunkMeta, nodeGroupIdx, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom);
        if (vectorToWriteFrom->isNull(posInVectorToWriteFrom)) {
            propertyStatistics.setHasNull(DUMMY_WRITE_TRANSACTION);
        }
        if (offsetInChunk >= chunkMeta.numValues) {
            chunkMeta.numValues = offsetInChunk + 1;
            metadataDA->update(nodeGroupIdx, chunkMeta);
        }
    }

    bool canCommitInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
        LocalVectorCollection* localChunk, const offset_to_row_idx_t& insertInfo,
        const offset_to_row_idx_t& updateInfo) override {
        auto metadata = getMetadata(nodeGroupIdx, transaction->getType());
        if (metadata.compMeta.canAlwaysUpdateInPlace()) {
            return true;
        }
        std::vector<row_idx_t> rowIdxesToRead;
        for (auto& [_, rowIdx] : updateInfo) {
            rowIdxesToRead.push_back(rowIdx);
        }
        for (auto& [_, rowIdx] : insertInfo) {
            rowIdxesToRead.push_back(rowIdx);
        }
        std::sort(rowIdxesToRead.begin(), rowIdxesToRead.end());
        for (auto rowIdx : rowIdxesToRead) {
            auto localVector = localChunk->getLocalVector(rowIdx);
            auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
            bool value = localVector->getVector()->isNull(offsetInVector);
            if (!metadata.compMeta.canUpdateInPlace(
                    reinterpret_cast<const uint8_t*>(&value), 0, dataType->getPhysicalType())) {
                return false;
            }
        }
        return true;
    }

protected:
    void commitLocalChunkInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
        LocalVectorCollection* localChunk, const offset_to_row_idx_t& insertInfo,
        const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) override {
        Column::commitLocalChunkInPlace(
            transaction, nodeGroupIdx, localChunk, insertInfo, updateInfo, deleteInfo);
        // Set nulls based on deleteInfo. Note that this code path actually only gets executed when
        // the column is a regular format one. This is not a good design, should be unified with csr
        // one in the future.
        for (auto offsetInChunk : deleteInfo) {
            setNull(nodeGroupIdx, offsetInChunk);
        }
    }

private:
    std::unique_ptr<ColumnChunk> getEmptyChunkForCommit() override {
        return ColumnChunkFactory::createNullColumnChunk(enableCompression);
    }
};

class SerialColumn final : public Column {
public:
    SerialColumn(std::string name, const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal, Transaction* transaction)
        : Column{name, LogicalType::SERIAL(), metaDAHeaderInfo, dataFH, metadataFH,
              // Serials can't be null, so they don't need PropertyStatistics
              bufferManager, wal, transaction, RWPropertyStats::empty(),
              false /* enableCompression */, false /*requireNullColumn*/} {}

    void scan(Transaction* /*transaction*/, ValueVector* nodeIDVector,
        ValueVector* resultVector) override {
        // Serial column cannot contain null values.
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto offset = nodeIDVector->readNodeOffset(pos);
            KU_ASSERT(!resultVector->isNull(pos));
            resultVector->setValue<offset_t>(pos, offset);
        }
    }

    void lookup(Transaction* /*transaction*/, ValueVector* nodeIDVector,
        ValueVector* resultVector) override {
        // Serial column cannot contain null values.
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto offset = nodeIDVector->readNodeOffset(pos);
            resultVector->setValue<offset_t>(pos, offset);
        }
    }

    void append(ColumnChunk* /*columnChunk*/, uint64_t nodeGroupIdx) override {
        metadataDA->resize(nodeGroupIdx + 1);
    }
};

InternalIDColumn::InternalIDColumn(std::string name, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, RWPropertyStats stats)
    : Column{name, LogicalType::INTERNAL_ID(), metaDAHeaderInfo, dataFH, metadataFH, bufferManager,
          wal, transaction, stats, false /* enableCompression */},
      commonTableID{INVALID_TABLE_ID} {}

void InternalIDColumn::populateCommonTableID(ValueVector* resultVector) const {
    auto nodeIDs = ((internalID_t*)resultVector->getData());
    for (auto i = 0u; i < resultVector->state->selVector->selectedSize; i++) {
        auto pos = resultVector->state->selVector->selectedPositions[i];
        nodeIDs[pos].tableID = commonTableID;
    }
}

Column::Column(std::string name, std::unique_ptr<LogicalType> dataType,
    const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
    BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
    RWPropertyStats propertyStatistics, bool enableCompression, bool requireNullColumn)
    : name{std::move(name)}, dbFileID{DBFileID::newDataFileID()}, dataType{std::move(dataType)},
      dataFH{dataFH}, metadataFH{metadataFH}, bufferManager{bufferManager}, wal{wal},
      propertyStatistics{propertyStatistics}, enableCompression{enableCompression} {
    metadataDA = std::make_unique<InMemDiskArray<ColumnChunkMetadata>>(*metadataFH,
        DBFileID::newMetadataFileID(), metaDAHeaderInfo.dataDAHPageIdx, bufferManager, wal,
        transaction);
    numBytesPerFixedSizedValue = getDataTypeSizeInChunk(*this->dataType);
    readToVectorFunc = getReadValuesToVectorFunc(*this->dataType);
    readToPageFunc = ReadCompressedValuesFromPage(*this->dataType);
    batchLookupFunc = ReadCompressedValuesFromPage(*this->dataType);
    writeFromVectorFunc = getWriteValueFromVectorFunc(*this->dataType);
    writeFunc = getWriteValuesFunc(*this->dataType);
    KU_ASSERT(numBytesPerFixedSizedValue <= BufferPoolConstants::PAGE_4KB_SIZE);
    if (requireNullColumn) {
        auto columnName =
            StorageUtils::getColumnName(this->name, StorageUtils::ColumnType::NULL_MASK, "");
        nullColumn =
            std::make_unique<NullColumn>(columnName, metaDAHeaderInfo.nullDAHPageIdx, dataFH,
                metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
}

// NOTE: This function should only be called on node tables.
void Column::batchLookup(
    Transaction* transaction, const offset_t* nodeOffsets, size_t size, uint8_t* result) {
    for (auto i = 0u; i < size; ++i) {
        auto nodeOffset = nodeOffsets[i];
        auto [nodeGroupIdx, offsetInChunk] =
            StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
        auto cursor = getPageCursorForOffset(transaction->getType(), nodeGroupIdx, offsetInChunk);
        auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
        KU_ASSERT(isPageIdxValid(cursor.pageIdx, chunkMeta));
        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            batchLookupFunc(frame, cursor, result, i, 1, chunkMeta.compMeta);
        });
    }
}

void Column::scan(Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (nullColumn) {
        nullColumn->scan(transaction, nodeIDVector, resultVector);
    }
    scanInternal(transaction, nodeIDVector, resultVector);
}

void Column::scan(transaction::Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t startOffsetInGroup, offset_t endOffsetInGroup, ValueVector* resultVector,
    uint64_t offsetInVector) {
    if (nullColumn) {
        nullColumn->scan(transaction, nodeGroupIdx, startOffsetInGroup, endOffsetInGroup,
            resultVector, offsetInVector);
    }
    auto state = getReadState(transaction->getType(), nodeGroupIdx);
    auto pageCursor = getPageCursorForOffsetInGroup(startOffsetInGroup, state);
    auto numValuesToScan = endOffsetInGroup - startOffsetInGroup;
    scanUnfiltered(
        transaction, pageCursor, numValuesToScan, resultVector, state.metadata, offsetInVector);
}

void Column::scan(
    Transaction* transaction, node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk) {
    if (nullColumn) {
        nullColumn->scan(transaction, nodeGroupIdx, columnChunk->getNullChunk());
    }
    if (nodeGroupIdx >= metadataDA->getNumElements(transaction->getType())) {
        columnChunk->setNumValues(0);
    } else {
        auto chunkMetadata = metadataDA->get(nodeGroupIdx, transaction->getType());
        auto cursor = PageElementCursor(chunkMetadata.pageIdx, 0);
        uint64_t numValuesPerPage =
            chunkMetadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, *dataType);
        uint64_t numValuesScanned = 0u;
        if (chunkMetadata.numValues > columnChunk->getCapacity()) {
            columnChunk->resize(std::bit_ceil(chunkMetadata.numValues));
        }
        auto numValuesToScan = chunkMetadata.numValues;
        KU_ASSERT(chunkMetadata.numValues <= columnChunk->getCapacity());
        while (numValuesScanned < numValuesToScan) {
            auto numValuesToReadInPage =
                std::min(numValuesPerPage, numValuesToScan - numValuesScanned);
            KU_ASSERT(isPageIdxValid(cursor.pageIdx, chunkMetadata));
            readFromPage(&DUMMY_READ_TRANSACTION, cursor.pageIdx, [&](uint8_t* frame) -> void {
                readToPageFunc(frame, cursor, columnChunk->getData(), numValuesScanned,
                    numValuesToReadInPage, chunkMetadata.compMeta);
            });
            numValuesScanned += numValuesToReadInPage;
            cursor.nextPage();
        }
        columnChunk->setNumValues(chunkMetadata.numValues);
    }
}

void Column::scan(Transaction* transaction, const ReadState& state, offset_t startOffsetInGroup,
    offset_t endOffsetInGroup, uint8_t* result) {
    auto cursor = getPageCursorForOffsetInGroup(startOffsetInGroup, state);
    auto numValuesToScan = endOffsetInGroup - startOffsetInGroup;
    uint64_t numValuesScanned = 0;
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)state.numValuesPerPage - cursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            readToPageFunc(frame, cursor, result, numValuesScanned, numValuesToScanInPage,
                state.metadata.compMeta);
        });
        numValuesScanned += numValuesToScanInPage;
        cursor.nextPage();
    }
}

void Column::scanInternal(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    auto startNodeOffset = nodeIDVector->readNodeOffset(0);
    KU_ASSERT(startNodeOffset % DEFAULT_VECTOR_CAPACITY == 0);
    // TODO: replace with state
    auto [nodeGroupIdx, offsetInChunk] =
        StorageUtils::getNodeGroupIdxAndOffsetInChunk(startNodeOffset);
    auto cursor = getPageCursorForOffset(transaction->getType(), nodeGroupIdx, offsetInChunk);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    if (nodeIDVector->state->selVector->isUnfiltered()) {
        scanUnfiltered(transaction, cursor, nodeIDVector->state->selVector->selectedSize,
            resultVector, chunkMeta);
    } else {
        scanFiltered(transaction, cursor, nodeIDVector, resultVector, chunkMeta);
    }
}

void Column::scanUnfiltered(Transaction* transaction, PageElementCursor& pageCursor,
    uint64_t numValuesToScan, ValueVector* resultVector, const ColumnChunkMetadata& chunkMeta,
    uint64_t startPosInVector) {
    uint64_t numValuesScanned = 0;
    auto numValuesPerPage =
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, *dataType);
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, chunkMeta));
        readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
            readToVectorFunc(frame, pageCursor, resultVector, numValuesScanned + startPosInVector,
                numValuesToScanInPage, chunkMeta.compMeta);
        });
        numValuesScanned += numValuesToScanInPage;
        pageCursor.nextPage();
    }
}

void Column::scanFiltered(Transaction* transaction, PageElementCursor& pageCursor,
    ValueVector* nodeIDVector, ValueVector* resultVector, const ColumnChunkMetadata& chunkMeta) {
    auto numValuesToScan = nodeIDVector->state->getOriginalSize();
    auto numValuesScanned = 0u;
    auto posInSelVector = 0u;
    auto numValuesPerPage =
        chunkMeta.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, *dataType);
    while (numValuesScanned < numValuesToScan) {
        uint64_t numValuesToScanInPage =
            std::min((uint64_t)numValuesPerPage - pageCursor.elemPosInPage,
                numValuesToScan - numValuesScanned);
        if (isInRange(nodeIDVector->state->selVector->selectedPositions[posInSelVector],
                numValuesScanned, numValuesScanned + numValuesToScanInPage)) {
            KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, chunkMeta));
            readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
                readToVectorFunc(frame, pageCursor, resultVector, numValuesScanned,
                    numValuesToScanInPage, chunkMeta.compMeta);
            });
        }
        numValuesScanned += numValuesToScanInPage;
        pageCursor.nextPage();
        while (
            posInSelVector < nodeIDVector->state->selVector->selectedSize &&
            nodeIDVector->state->selVector->selectedPositions[posInSelVector] < numValuesScanned) {
            posInSelVector++;
        }
    }
}

void Column::lookup(
    Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    if (nullColumn) {
        nullColumn->lookup(transaction, nodeIDVector, resultVector);
    }
    lookupInternal(transaction, nodeIDVector, resultVector);
}

void Column::lookupInternal(
    transaction::Transaction* transaction, ValueVector* nodeIDVector, ValueVector* resultVector) {
    for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
        auto pos = nodeIDVector->state->selVector->selectedPositions[i];
        if (nodeIDVector->isNull(pos)) {
            continue;
        }
        auto nodeOffset = nodeIDVector->readNodeOffset(pos);
        lookupValue(transaction, nodeOffset, resultVector, pos);
    }
}

void Column::lookupValue(transaction::Transaction* transaction, offset_t nodeOffset,
    ValueVector* resultVector, uint32_t posInVector) {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    auto cursor = getPageCursorForOffset(transaction->getType(), nodeGroupIdx, offsetInChunk);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
    KU_ASSERT(isPageIdxValid(cursor.pageIdx, chunkMeta));
    readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
        readToVectorFunc(
            frame, cursor, resultVector, posInVector, 1 /* numValuesToRead */, chunkMeta.compMeta);
    });
}

void Column::readFromPage(
    Transaction* transaction, page_idx_t pageIdx, const std::function<void(uint8_t*)>& func) {
    // For constant compression, call read on a nullptr since there is no data on disk and
    // decompression only requires metadata
    if (pageIdx == INVALID_PAGE_IDX) {
        return func(nullptr);
    }
    auto [fileHandleToPin, pageIdxToPin] = DBFileUtils::getFileHandleAndPhysicalPageIdxToPin(
        *dataFH, pageIdx, *wal, transaction->getType());
    bufferManager->optimisticRead(*fileHandleToPin, pageIdxToPin, func);
}

void Column::append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) {
    // Main column chunk.
    auto preScanMetadata = columnChunk->getMetadataToFlush();
    auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);
    auto metadata = columnChunk->flushBuffer(dataFH, startPageIdx, preScanMetadata);
    metadataDA->resize(nodeGroupIdx + 1);
    metadataDA->update(nodeGroupIdx, metadata);
    if (nullColumn) {
        // Null column chunk.
        nullColumn->append(columnChunk->getNullChunk(), nodeGroupIdx);
    }
}

void Column::write(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk,
    ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    bool isNull = vectorToWriteFrom->isNull(posInVectorToWriteFrom);
    auto chunkMeta = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
    if (!isNull) {
        writeValue(
            chunkMeta, nodeGroupIdx, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom);
    }
    if (offsetInChunk >= chunkMeta.numValues) {
        chunkMeta.numValues = offsetInChunk + 1;
        metadataDA->update(nodeGroupIdx, chunkMeta);
    }
}

void Column::writeValue(const ColumnChunkMetadata& chunkMeta, node_group_idx_t nodeGroupIdx,
    offset_t offsetInChunk, ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) {
    auto walPageInfo = createWALVersionOfPageForValue(nodeGroupIdx, offsetInChunk);
    KU_ASSERT(isPageIdxValid(walPageInfo.originalPageIdx, chunkMeta));
    try {
        writeFromVectorFunc(walPageInfo.frame, walPageInfo.posInPage, vectorToWriteFrom,
            posInVectorToWriteFrom, chunkMeta.compMeta);
    } catch (Exception& e) {
        DBFileUtils::unpinWALPageAndReleaseOriginalPageLock(
            walPageInfo, *dataFH, *bufferManager, *wal);
        throw;
    }
    DBFileUtils::unpinWALPageAndReleaseOriginalPageLock(walPageInfo, *dataFH, *bufferManager, *wal);
}

void Column::writeValue(const ColumnChunkMetadata& chunkMeta, node_group_idx_t nodeGroupIdx,
    offset_t offsetInChunk, const uint8_t* data) {
    auto walPageInfo = createWALVersionOfPageForValue(nodeGroupIdx, offsetInChunk);
    try {
        writeFunc(walPageInfo.frame, walPageInfo.posInPage, data, 0, 1, chunkMeta.compMeta);
    } catch (Exception& e) {
        DBFileUtils::unpinWALPageAndReleaseOriginalPageLock(
            walPageInfo, *dataFH, *bufferManager, *wal);
        throw;
    }
    DBFileUtils::unpinWALPageAndReleaseOriginalPageLock(walPageInfo, *dataFH, *bufferManager, *wal);
}

offset_t Column::appendValues(
    node_group_idx_t nodeGroupIdx, const uint8_t* data, offset_t numValues) {
    auto state = getReadState(TransactionType::WRITE, nodeGroupIdx);
    auto startOffset = state.metadata.numValues;
    auto cursor = getPageCursorForOffsetInGroup(startOffset, state);
    offset_t valuesWritten = 0;
    while (valuesWritten < numValues) {
        bool insertingNewPage = false;
        if (cursor.pageIdx >= dataFH->getNumPages()) {
            KU_ASSERT(cursor.pageIdx == dataFH->getNumPages());
            DBFileUtils::insertNewPage(*dataFH, dbFileID, *bufferManager, *wal);
            insertingNewPage = true;
        }

        uint64_t numValuesToWriteInPage = std::min(
            (uint64_t)state.numValuesPerPage - cursor.elemPosInPage, numValues - valuesWritten);
        auto walPageInfo = DBFileUtils::createWALVersionIfNecessaryAndPinPage(
            cursor.pageIdx, insertingNewPage, *dataFH, dbFileID, *bufferManager, *wal);

        try {
            writeFunc(walPageInfo.frame, cursor.elemPosInPage, data, 0, numValuesToWriteInPage,
                state.metadata.compMeta);
        } catch (Exception& e) {
            DBFileUtils::unpinWALPageAndReleaseOriginalPageLock(
                walPageInfo, *dataFH, *bufferManager, *wal);
            throw;
        }
        DBFileUtils::unpinWALPageAndReleaseOriginalPageLock(
            walPageInfo, *dataFH, *bufferManager, *wal);
        valuesWritten += numValuesToWriteInPage;
        if (valuesWritten < numValues) {
            cursor.nextPage();
            state.metadata.numPages++;
        }
    }
    state.metadata.numValues += numValues;
    metadataDA->update(nodeGroupIdx, state.metadata);
    return startOffset;
}

PageElementCursor Column::getPageCursorForOffsetInGroup(
    offset_t offsetInChunk, const ReadState& state) {
    auto pageCursor = PageUtils::getPageElementCursorForPos(offsetInChunk, state.numValuesPerPage);
    pageCursor.pageIdx += state.metadata.pageIdx;
    return pageCursor;
}

WALPageIdxPosInPageAndFrame Column::createWALVersionOfPageForValue(
    node_group_idx_t nodeGroupIdx, offset_t offsetInChunk) {
    auto originalPageCursor =
        getPageCursorForOffset(TransactionType::WRITE, nodeGroupIdx, offsetInChunk);
    bool insertingNewPage = false;
    if (originalPageCursor.pageIdx == INVALID_PAGE_IDX) {
        return {{INVALID_PAGE_IDX, INVALID_PAGE_IDX, nullptr}, originalPageCursor.elemPosInPage};
    } else if (originalPageCursor.pageIdx >= dataFH->getNumPages()) {
        KU_ASSERT(originalPageCursor.pageIdx == dataFH->getNumPages());
        DBFileUtils::insertNewPage(*dataFH, dbFileID, *bufferManager, *wal);
        insertingNewPage = true;
    }
    auto walPageIdxAndFrame = DBFileUtils::createWALVersionIfNecessaryAndPinPage(
        originalPageCursor.pageIdx, insertingNewPage, *dataFH, dbFileID, *bufferManager, *wal);
    return {walPageIdxAndFrame, originalPageCursor.elemPosInPage};
}

Column::ReadState Column::getReadState(
    TransactionType transactionType, node_group_idx_t nodeGroupIdx) const {
    auto metadata = metadataDA->get(nodeGroupIdx, transactionType);
    return {metadata, metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, *dataType)};
}

bool Column::isNull(
    Transaction* transaction, node_group_idx_t nodeGroupIdx, offset_t offsetInChunk) {
    if (nullColumn) {
        return nullColumn->isNull(transaction, nodeGroupIdx, offsetInChunk);
    }
    return false;
}

void Column::setNull(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk) {
    if (nullColumn) {
        nullColumn->setNull(nodeGroupIdx, offsetInChunk);
    }
}

void Column::prepareCommitForChunk(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    LocalVectorCollection* localColumnChunk, const offset_to_row_idx_t& insertInfo,
    const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) {
    auto currentNumNodeGroups = metadataDA->getNumElements(transaction->getType());
    auto isNewNodeGroup = nodeGroupIdx >= currentNumNodeGroups;
    if (isNewNodeGroup) {
        // If this is a new node group, updateInfo should be empty. We should perform out-of-place
        // commit with a new column chunk.
        commitLocalChunkOutOfPlace(transaction, nodeGroupIdx, localColumnChunk, isNewNodeGroup,
            insertInfo, updateInfo, deleteInfo);
    } else {
        bool didInPlaceCommit = false;
        // If this is not a new node group, we should first check if we can perform in-place commit.
        if (canCommitInPlace(transaction, nodeGroupIdx, localColumnChunk, insertInfo, updateInfo)) {
            commitLocalChunkInPlace(
                transaction, nodeGroupIdx, localColumnChunk, insertInfo, updateInfo, deleteInfo);
            didInPlaceCommit = true;
        } else {
            commitLocalChunkOutOfPlace(transaction, nodeGroupIdx, localColumnChunk, isNewNodeGroup,
                insertInfo, updateInfo, deleteInfo);
        }
        if (nullColumn) {
            // Uses functions written for the nullchunk which only access the localColumnChunk's
            // null information
            if (nullColumn->canCommitInPlace(
                    transaction, nodeGroupIdx, localColumnChunk, insertInfo, updateInfo)) {
                nullColumn->commitLocalChunkInPlace(transaction, nodeGroupIdx, localColumnChunk,
                    insertInfo, updateInfo, deleteInfo);
            } else if (didInPlaceCommit) {
                // Out-of-place commits also commit the null chunk out of place,
                // so we only need to do a separate out of place commit for the null chunk if the
                // main chunk did an in-place commit.
                nullColumn->commitLocalChunkOutOfPlace(transaction, nodeGroupIdx, localColumnChunk,
                    isNewNodeGroup, insertInfo, updateInfo, deleteInfo);
            }
        }
    }
}

bool Column::containsVarList(LogicalType& dataType) {
    switch (dataType.getPhysicalType()) {
    case PhysicalTypeID::VAR_LIST: {
        return true;
    }
    case PhysicalTypeID::STRUCT: {
        for (auto field : StructType::getFieldTypes(&dataType)) {
            if (containsVarList(*field)) {
                return true;
            }
        }
    } break;
    default:
        return false;
    }
    return false;
}

// TODO(Guodong): This should be moved inside `LocalVectorCollection`.
bool Column::canCommitInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    LocalVectorCollection* localChunk, const offset_to_row_idx_t& insertInfo,
    const offset_to_row_idx_t& updateInfo) {
    if (containsVarList(*dataType)) {
        // Always perform out of place commit for VAR_LIST data type.
        return false;
    }
    auto metadata = getMetadata(nodeGroupIdx, transaction->getType());
    if (metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    std::vector<row_idx_t> rowIdxesToRead;
    for (auto& [_, rowIdx] : updateInfo) {
        rowIdxesToRead.push_back(rowIdx);
    }
    for (auto& [_, rowIdx] : insertInfo) {
        rowIdxesToRead.push_back(rowIdx);
    }
    std::sort(rowIdxesToRead.begin(), rowIdxesToRead.end());
    for (auto rowIdx : rowIdxesToRead) {
        auto localVector = localChunk->getLocalVector(rowIdx);
        auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
        if (!metadata.compMeta.canUpdateInPlace(
                localVector->getVector()->getData(), offsetInVector, dataType->getPhysicalType())) {
            return false;
        }
    }
    return true;
}

void Column::commitLocalChunkInPlace(Transaction* /*transaction*/, node_group_idx_t nodeGroupIdx,
    LocalVectorCollection* localChunk, const offset_to_row_idx_t& insertInfo,
    const offset_to_row_idx_t& updateInfo, const offset_set_t& /*deleteInfo*/) {
    applyLocalChunkToColumn(nodeGroupIdx, localChunk, updateInfo);
    applyLocalChunkToColumn(nodeGroupIdx, localChunk, insertInfo);
}

std::unique_ptr<ColumnChunk> Column::getEmptyChunkForCommit() {
    return ColumnChunkFactory::createColumnChunk(dataType->copy(), enableCompression);
}

void Column::commitLocalChunkOutOfPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    LocalVectorCollection* localChunk, bool isNewNodeGroup, const offset_to_row_idx_t& insertInfo,
    const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) {
    auto columnChunk = getEmptyChunkForCommit();
    if (isNewNodeGroup) {
        KU_ASSERT(updateInfo.empty() && deleteInfo.empty());
        // Apply inserts from the local chunk.
        applyLocalChunkToColumnChunk(localChunk, columnChunk.get(), insertInfo);
    } else {
        // First, scan the whole column chunk from persistent storage.
        scan(transaction, nodeGroupIdx, columnChunk.get());
        // Then, apply updates from the local chunk.
        applyLocalChunkToColumnChunk(localChunk, columnChunk.get(), updateInfo);
        // Lastly, apply inserts from the local chunk.
        applyLocalChunkToColumnChunk(localChunk, columnChunk.get(), insertInfo);
        if (columnChunk->getNullChunk()) {
            // Set nulls based on deleteInfo.
            for (auto offsetInChunk : deleteInfo) {
                columnChunk->getNullChunk()->setNull(offsetInChunk, true /* isNull */);
            }
        }
    }
    columnChunk->finalize();
    append(columnChunk.get(), nodeGroupIdx);
}

void Column::applyLocalChunkToColumnChunk(LocalVectorCollection* localChunk,
    ColumnChunk* columnChunk, const std::map<offset_t, row_idx_t>& updateInfo) {
    for (auto& [offsetInChunk, rowIdx] : updateInfo) {
        auto localVector = localChunk->getLocalVector(rowIdx);
        auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
        localVector->getVector()->state->selVector->selectedPositions[0] = offsetInVector;
        columnChunk->write(localVector->getVector(), offsetInVector, offsetInChunk);
    }
}

void Column::applyLocalChunkToColumn(node_group_idx_t nodeGroupIdx,
    LocalVectorCollection* localChunk, const std::map<offset_t, row_idx_t>& updateInfo) {
    for (auto& [offsetInChunk, rowIdx] : updateInfo) {
        auto localVector = localChunk->getLocalVector(rowIdx);
        auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
        write(nodeGroupIdx, offsetInChunk, localVector->getVector(), offsetInVector);
    }
}

void Column::checkpointInMemory() {
    metadataDA->checkpointInMemoryIfNecessary();
    if (nullColumn) {
        nullColumn->checkpointInMemory();
    }
}

void Column::rollbackInMemory() {
    metadataDA->rollbackInMemoryIfNecessary();
    if (nullColumn) {
        nullColumn->rollbackInMemory();
    }
}

void Column::populateWithDefaultVal(const Property& property, Column* column,
    InMemDiskArray<ColumnChunkMetadata>* metadataDA_, ValueVector* defaultValueVector,
    uint64_t numNodeGroups) const {
    auto columnChunk =
        ColumnChunkFactory::createColumnChunk(property.getDataType()->copy(), enableCompression);
    columnChunk->populateWithDefaultVal(defaultValueVector);
    for (auto i = 0u; i < numNodeGroups; i++) {
        auto chunkMeta = metadataDA_->get(i, TransactionType::WRITE);
        auto capacity = columnChunk->getCapacity();
        while (capacity < chunkMeta.numValues) {
            capacity *= CHUNK_RESIZE_RATIO;
        }
        if (capacity > columnChunk->getCapacity()) {
            auto newColumnChunk = ColumnChunkFactory::createColumnChunk(
                property.getDataType()->copy(), enableCompression);
            newColumnChunk->populateWithDefaultVal(defaultValueVector);
            newColumnChunk->setNumValues(chunkMeta.numValues);
            column->append(newColumnChunk.get(), i);
        } else {
            columnChunk->setNumValues(chunkMeta.numValues);
            column->append(columnChunk.get(), i);
        }
    }
}

PageElementCursor Column::getPageCursorForOffset(
    TransactionType transactionType, node_group_idx_t nodeGroupIdx, offset_t offsetInChunk) {
    auto state = getReadState(transactionType, nodeGroupIdx);
    return getPageCursorForOffsetInGroup(offsetInChunk, state);
}

std::unique_ptr<Column> ColumnFactory::createColumn(std::string name,
    std::unique_ptr<LogicalType> dataType, const MetadataDAHInfo& metaDAHeaderInfo,
    BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
    transaction::Transaction* transaction, RWPropertyStats propertyStatistics,
    bool enableCompression) {
    switch (dataType->getLogicalTypeID()) {
    case LogicalTypeID::BOOL:
    case LogicalTypeID::INT64:
    case LogicalTypeID::INT32:
    case LogicalTypeID::INT16:
    case LogicalTypeID::INT8:
    case LogicalTypeID::UINT64:
    case LogicalTypeID::UINT32:
    case LogicalTypeID::UINT16:
    case LogicalTypeID::UINT8:
    case LogicalTypeID::INT128:
    case LogicalTypeID::DOUBLE:
    case LogicalTypeID::FLOAT:
    case LogicalTypeID::DATE:
    case LogicalTypeID::TIMESTAMP:
    case LogicalTypeID::TIMESTAMP_MS:
    case LogicalTypeID::TIMESTAMP_NS:
    case LogicalTypeID::TIMESTAMP_SEC:
    case LogicalTypeID::TIMESTAMP_TZ:
    case LogicalTypeID::INTERVAL:
    case LogicalTypeID::FIXED_LIST: {
        return std::make_unique<Column>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case LogicalTypeID::INTERNAL_ID: {
        return std::make_unique<InternalIDColumn>(name, metaDAHeaderInfo, dataFH, metadataFH,
            bufferManager, wal, transaction, propertyStatistics);
    }
    case LogicalTypeID::BLOB:
    case LogicalTypeID::STRING: {
        return std::make_unique<StringColumn>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case LogicalTypeID::MAP:
    case LogicalTypeID::VAR_LIST: {
        return std::make_unique<VarListColumn>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case LogicalTypeID::UNION:
    case LogicalTypeID::STRUCT:
    case LogicalTypeID::RDF_VARIANT: {
        return std::make_unique<StructColumn>(name, std::move(dataType), metaDAHeaderInfo, dataFH,
            metadataFH, bufferManager, wal, transaction, propertyStatistics, enableCompression);
    }
    case LogicalTypeID::SERIAL: {
        return std::make_unique<SerialColumn>(
            name, metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal, transaction);
    }
    default: {
        KU_UNREACHABLE;
    }
    }
}

} // namespace storage
} // namespace kuzu
