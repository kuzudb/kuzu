#include "storage/store/column.h"

#include <memory>

#include "common/assert.h"
#include "storage/stats/property_statistics.h"
#include "storage/store/column_chunk.h"
#include "storage/store/null_column.h"
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
    void operator()(const uint8_t* frame, PageCursor& pageCursor, ValueVector* resultVector,
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
        compressedWriter(frame, posInFrame, data, dataOffset, numValues, metadata);
    }
    void operator()(uint8_t* frame, uint16_t posInFrame, ValueVector* vector,
        uint32_t offsetInVector, const CompressionMetadata& metadata) {
        KU_ASSERT(vector->dataType.getPhysicalType() == PhysicalTypeID::INTERNAL_ID);
        compressedWriter(frame, posInFrame,
            reinterpret_cast<const uint8_t*>(
                &vector->getValue<internalID_t>(offsetInVector).offset),
            0 /*dataOffset*/, 1 /*numValues*/, metadata);
    }

private:
    WriteCompressedValuesToPage compressedWriter;
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
            resultVector->setNull(pos, false);
            resultVector->setValue<offset_t>(pos, offset);
        }
    }

    void lookup(Transaction* /*transaction*/, ValueVector* nodeIDVector,
        ValueVector* resultVector) override {
        // Serial column cannot contain null values.
        for (auto i = 0ul; i < nodeIDVector->state->selVector->selectedSize; i++) {
            auto pos = nodeIDVector->state->selVector->selectedPositions[i];
            auto offset = nodeIDVector->readNodeOffset(pos);
            resultVector->setNull(pos, false);
            resultVector->setValue<offset_t>(pos, offset);
        }
    }

    bool canCommitInPlace(Transaction* /*transaction*/, node_group_idx_t /*nodeGroupIdx*/,
        LocalVectorCollection* /*localChunk*/, const offset_to_row_idx_t& /*insertInfo*/,
        const offset_to_row_idx_t& updateInfo) override {
        KU_ASSERT(updateInfo.empty());
        return true;
    }

    bool canCommitInPlace(transaction::Transaction* /*transaction*/,
        common::node_group_idx_t /*nodeGroupIdx*/, common::offset_t /*dstOffset*/,
        ColumnChunk* /*chunk*/, common::offset_t /*startOffset*/,
        common::length_t /*length*/) override {
        // Note: only updates to rel table can trigger this code path. SERIAL is not supported in
        // rel table yet.
        KU_UNREACHABLE;
    }

    void commitLocalChunkInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
        LocalVectorCollection* /*localChunk*/, const offset_to_row_idx_t& insertInfo,
        const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) override {
        KU_ASSERT(updateInfo.empty() && deleteInfo.empty());
        auto chunkMeta = metadataDA->get(nodeGroupIdx, transaction->getType());
        auto numValues = chunkMeta.numValues;
        for (auto& [offsetInChunk, _] : insertInfo) {
            if (offsetInChunk >= numValues) {
                numValues = offsetInChunk + 1;
            }
        }
        if (numValues > chunkMeta.numValues) {
            chunkMeta.numValues = numValues;
            metadataDA->update(nodeGroupIdx, chunkMeta);
        }
    }

    void commitLocalChunkOutOfPlace(Transaction* /*transaction*/, node_group_idx_t nodeGroupIdx,
        LocalVectorCollection* /*localChunk*/, bool isNewNodeGroup,
        const offset_to_row_idx_t& insertInfo, const offset_to_row_idx_t& updateInfo,
        const offset_set_t& deleteInfo) override {
        KU_ASSERT(isNewNodeGroup && updateInfo.empty() && deleteInfo.empty());
        // Only when a new node group is created, we need to commit out of place.
        auto numValues = 0u;
        for (auto& [offsetInChunk, _] : insertInfo) {
            numValues = offsetInChunk >= numValues ? offsetInChunk + 1 : numValues;
        }
        ColumnChunkMetadata chunkMeta;
        chunkMeta.numValues = numValues;
        metadataDA->resize(nodeGroupIdx + 1);
        metadataDA->update(nodeGroupIdx, chunkMeta);
    }

    void commitColumnChunkInPlace(common::node_group_idx_t /*nodeGroupIdx*/,
        common::offset_t /*dstOffset*/, ColumnChunk* /*chunk*/,
        common::offset_t /*srcOffsetInChunk*/, uint64_t /*numValues*/) override {
        // Note: only updates to rel table can trigger this code path. SERIAL is not supported in
        // rel table yet.
        KU_UNREACHABLE;
    }

    void commitColumnChunkOutOfPlace(Transaction* /*transaction*/,
        common::node_group_idx_t /*nodeGroupIdx*/, bool /*isNewNodeGroup*/,
        common::offset_t /*dstOffset*/, ColumnChunk* /*chunk*/,
        common::offset_t /*srcOffsetInChunk*/, uint64_t /*numValues*/) override {
        // Note: only updates to rel table can trigger this code path. SERIAL is not supported in
        // rel table yet.
        KU_UNREACHABLE;
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

Column::~Column() = default;

Column* Column::getNullColumn() {
    return nullColumn.get();
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

void Column::scan(Transaction* transaction, node_group_idx_t nodeGroupIdx, ColumnChunk* columnChunk,
    offset_t startOffset, offset_t endOffset) {
    if (nullColumn) {
        nullColumn->scan(
            transaction, nodeGroupIdx, columnChunk->getNullChunk(), startOffset, endOffset);
    }
    if (nodeGroupIdx >= metadataDA->getNumElements(transaction->getType())) {
        columnChunk->setNumValues(0);
    } else {
        auto chunkMetadata = metadataDA->get(nodeGroupIdx, transaction->getType());
        if (chunkMetadata.numValues == 0) {
            columnChunk->setNumValues(0);
            return;
        }
        uint64_t numValuesPerPage =
            chunkMetadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, *dataType);
        auto cursor = PageUtils::getPageCursorForPos(startOffset, numValuesPerPage);
        cursor.pageIdx += chunkMetadata.pageIdx;
        uint64_t numValuesScanned = 0u;
        endOffset = std::min(endOffset, chunkMetadata.numValues);
        KU_ASSERT(endOffset >= startOffset);
        auto numValuesToScan = endOffset - startOffset;
        if (numValuesToScan > columnChunk->getCapacity()) {
            columnChunk->resize(std::bit_ceil(numValuesToScan));
        }
        KU_ASSERT((numValuesToScan + startOffset) <= chunkMetadata.numValues);
        while (numValuesScanned < numValuesToScan) {
            auto numValuesToReadInPage = std::min(
                numValuesPerPage - cursor.elemPosInPage, numValuesToScan - numValuesScanned);
            KU_ASSERT(isPageIdxValid(cursor.pageIdx, chunkMetadata));
            readFromPage(&DUMMY_READ_TRANSACTION, cursor.pageIdx, [&](uint8_t* frame) -> void {
                readToPageFunc(frame, cursor, columnChunk->getData(), numValuesScanned,
                    numValuesToReadInPage, chunkMetadata.compMeta);
            });
            numValuesScanned += numValuesToReadInPage;
            cursor.nextPage();
        }
        KU_ASSERT(numValuesScanned == numValuesToScan);
        columnChunk->setNumValues(numValuesScanned);
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

void Column::scanUnfiltered(Transaction* transaction, PageCursor& pageCursor,
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

void Column::scanFiltered(Transaction* transaction, PageCursor& pageCursor,
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

static bool sanityCheckForWrites(const ColumnChunkMetadata& metadata, const LogicalType& dataType) {
    if (metadata.compMeta.compression == CompressionType::CONSTANT) {
        return metadata.numPages == 0;
    } else {
        auto numValuesPerPage =
            metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, dataType);
        if (numValuesPerPage == UINT64_MAX) {
            return metadata.numPages == 0;
        } else {
            return std::ceil((double)metadata.numValues / (double)numValuesPerPage) <=
                   metadata.numPages;
        }
    }
}

void Column::append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) {
    // Main column chunk.
    auto preScanMetadata = columnChunk->getMetadataToFlush();
    auto startPageIdx = dataFH->addNewPages(preScanMetadata.numPages);
    auto metadata = columnChunk->flushBuffer(dataFH, startPageIdx, preScanMetadata);
    KU_ASSERT(sanityCheckForWrites(metadata, *dataType));
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
        KU_ASSERT(sanityCheckForWrites(chunkMeta, *dataType));
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

void Column::write(node_group_idx_t nodeGroupIdx, offset_t offsetInChunk, ColumnChunk* data,
    offset_t dataOffset, length_t numValues) {
    auto chunkMeta = metadataDA->get(nodeGroupIdx, TransactionType::WRITE);
    writeValues(chunkMeta, nodeGroupIdx, offsetInChunk, data->getData(), dataOffset, numValues);
    if (offsetInChunk + numValues > chunkMeta.numValues) {
        chunkMeta.numValues = offsetInChunk + numValues;
        KU_ASSERT(sanityCheckForWrites(chunkMeta, *dataType));
        metadataDA->update(nodeGroupIdx, chunkMeta);
    }
}

void Column::writeValues(const ColumnChunkMetadata& chunkMeta,
    common::node_group_idx_t nodeGroupIdx, common::offset_t dstOffset, const uint8_t* data,
    common::offset_t dataOffset, common::offset_t numValues) {
    auto numValuesWritten = 0u;
    auto state = getReadState(TransactionType::WRITE, nodeGroupIdx);
    while (numValuesWritten < numValues) {
        auto walPageInfo =
            createWALVersionOfPageForValue(nodeGroupIdx, dstOffset + numValuesWritten);
        auto numValuesToWriteInPage =
            std::min(numValues - numValuesWritten, state.numValuesPerPage - walPageInfo.posInPage);
        try {
            writeFunc(walPageInfo.frame, walPageInfo.posInPage, data, dataOffset + numValuesWritten,
                numValuesToWriteInPage, chunkMeta.compMeta);
        } catch (Exception& e) {
            DBFileUtils::unpinWALPageAndReleaseOriginalPageLock(
                walPageInfo, *dataFH, *bufferManager, *wal);
            throw;
        }
        DBFileUtils::unpinWALPageAndReleaseOriginalPageLock(
            walPageInfo, *dataFH, *bufferManager, *wal);
        numValuesWritten += numValuesToWriteInPage;
    }
}

offset_t Column::appendValues(
    node_group_idx_t nodeGroupIdx, const uint8_t* data, offset_t numValues) {
    auto state = getReadState(TransactionType::WRITE, nodeGroupIdx);
    auto startOffset = state.metadata.numValues;
    auto numPages = dataFH->getNumPages();
    writeValues(
        state.metadata, nodeGroupIdx, state.metadata.numValues, data, 0 /*dataOffset*/, numValues);
    auto newNumPages = dataFH->getNumPages();
    state.metadata.numValues += numValues;
    state.metadata.numPages += (newNumPages - numPages);
    metadataDA->update(nodeGroupIdx, state.metadata);
    return startOffset;
}

PageCursor Column::getPageCursorForOffsetInGroup(offset_t offsetInChunk, const ReadState& state) {
    auto pageCursor = PageUtils::getPageCursorForPos(offsetInChunk, state.numValuesPerPage);
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
        // TODO(Guodong/Ben): The logic here on NullColumn is confusing as out-of-place commits and
        // in-place commits handle it differently. See if we can unify them.
        if (nullColumn) {
            // Uses functions written for the null chunk which only access the localColumnChunk's
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

void Column::prepareCommitForChunk(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    offset_t csrOffset, ColumnChunk* chunk, offset_t dataOffset, length_t numValues) {
    auto currentNumNodeGroups = metadataDA->getNumElements(transaction->getType());
    auto isNewNodeGroup = nodeGroupIdx >= currentNumNodeGroups;
    if (isNewNodeGroup) {
        commitColumnChunkOutOfPlace(
            transaction, nodeGroupIdx, isNewNodeGroup, csrOffset, chunk, dataOffset, numValues);
    } else {
        bool didInPlaceCommit = false;
        // If this is not a new node group, we should first check if we can perform in-place commit.
        if (canCommitInPlace(transaction, nodeGroupIdx, csrOffset, chunk, dataOffset, numValues)) {
            commitColumnChunkInPlace(nodeGroupIdx, csrOffset, chunk, dataOffset, numValues);
            didInPlaceCommit = true;
        } else {
            commitColumnChunkOutOfPlace(
                transaction, nodeGroupIdx, isNewNodeGroup, csrOffset, chunk, dataOffset, numValues);
        }
        if (nullColumn) {
            if (nullColumn->canCommitInPlace(transaction, nodeGroupIdx, csrOffset,
                    chunk->getNullChunk(), dataOffset, numValues)) {
                nullColumn->commitColumnChunkInPlace(
                    nodeGroupIdx, csrOffset, chunk->getNullChunk(), dataOffset, numValues);
            } else if (didInPlaceCommit) {
                nullColumn->commitColumnChunkOutOfPlace(transaction, nodeGroupIdx, isNewNodeGroup,
                    csrOffset, chunk->getNullChunk(), dataOffset, numValues);
            }
        }
    }
}

bool Column::isInsertionsOutOfPagesCapacity(
    const ColumnChunkMetadata& metadata, const offset_to_row_idx_t& insertInfo) {
    auto maxOffset = 0u;
    for (auto& [offset, rowIdx] : insertInfo) {
        if (offset > maxOffset) {
            maxOffset = offset;
        }
    }
    if (metadata.compMeta.compression != CompressionType::CONSTANT &&
        (metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, *dataType) *
            metadata.numPages) <= (maxOffset + 1)) {
        // Note that for constant compression, `metadata.numPages` will be equal to 0. Thus, this
        // function will always return true.
        return true;
    }
    return false;
}

bool Column::checkUpdateInPlace(const ColumnChunkMetadata& metadata,
    LocalVectorCollection* localChunk, const offset_to_row_idx_t& insertInfo,
    const offset_to_row_idx_t& updateInfo) {
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
        if (localVector->getVector()->isNull(offsetInVector)) {
            continue;
        }
        if (!metadata.compMeta.canUpdateInPlace(
                localVector->getVector()->getData(), offsetInVector, dataType->getPhysicalType())) {
            return false;
        }
    }
    return true;
}

bool Column::canCommitInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    LocalVectorCollection* localChunk, const offset_to_row_idx_t& insertInfo,
    const offset_to_row_idx_t& updateInfo) {
    auto metadata = getMetadata(nodeGroupIdx, transaction->getType());
    if (isInsertionsOutOfPagesCapacity(metadata, insertInfo)) {
        return false;
    }
    if (metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    return checkUpdateInPlace(metadata, localChunk, insertInfo, updateInfo);
}

bool Column::canCommitInPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    common::offset_t dstOffset, ColumnChunk* chunk, common::offset_t dataOffset, length_t length) {
    auto metadata = getMetadata(nodeGroupIdx, transaction->getType());
    if (metadata.compMeta.compression != CompressionType::CONSTANT &&
        (metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, *dataType) *
            metadata.numPages) < (length + dstOffset)) {
        // Note that for constant compression, `metadata.numPages` will be equal to 0. Thus, this
        // function will always return false.
        return false;
    }
    if (metadata.compMeta.canAlwaysUpdateInPlace()) {
        return true;
    }
    for (auto i = 0u; i < length; i++) {
        if (!metadata.compMeta.canUpdateInPlace(
                chunk->getData(), dataOffset + i, dataType->getPhysicalType())) {
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

std::unique_ptr<ColumnChunk> Column::getEmptyChunkForCommit(uint64_t capacity) {
    return ColumnChunkFactory::createColumnChunk(dataType->copy(), enableCompression, capacity);
}

void Column::commitLocalChunkOutOfPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    LocalVectorCollection* localChunk, bool isNewNodeGroup, const offset_to_row_idx_t& insertInfo,
    const offset_to_row_idx_t& updateInfo, const offset_set_t& deleteInfo) {
    auto columnChunk = getEmptyChunkForCommit(common::StorageConstants::NODE_GROUP_SIZE);
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

void Column::commitColumnChunkInPlace(node_group_idx_t nodeGroupIdx, offset_t dstOffset,
    ColumnChunk* chunk, offset_t srcOffsetInChunk, uint64_t numValues) {
    write(nodeGroupIdx, dstOffset, chunk, srcOffsetInChunk, numValues);
}

void Column::commitColumnChunkOutOfPlace(Transaction* transaction, node_group_idx_t nodeGroupIdx,
    bool isNewNodeGroup, offset_t csrOffset, ColumnChunk* chunk, offset_t dataOffset,
    length_t numValues) {
    if (isNewNodeGroup) {
        chunk->finalize();
        append(chunk, nodeGroupIdx);
    } else {
        auto chunkMeta = getMetadata(nodeGroupIdx, transaction->getType());
        if (numValues < chunkMeta.numValues) {
            // TODO(Guodong): Should consider caching the scanned column chunk to avoid redundant
            // scans in the same transaction.
            auto columnChunk = getEmptyChunkForCommit(chunkMeta.numValues + numValues);
            scan(transaction, nodeGroupIdx, columnChunk.get());
            columnChunk->write(chunk, dataOffset, csrOffset, numValues);
            columnChunk->finalize();
            append(columnChunk.get(), nodeGroupIdx);
        } else {
            chunk->finalize();
            append(chunk, nodeGroupIdx);
        }
    }
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
    LocalVectorCollection* localChunk, const offset_to_row_idx_t& updateInfo) {
    for (auto& [offsetInChunk, rowIdx] : updateInfo) {
        auto localVector = localChunk->getLocalVector(rowIdx);
        auto offsetInVector = rowIdx & (DEFAULT_VECTOR_CAPACITY - 1);
        if (!localVector->getVector()->isNull(offsetInVector)) {
            write(nodeGroupIdx, offsetInChunk, localVector->getVector(), offsetInVector);
        }
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

PageCursor Column::getPageCursorForOffset(
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
    case LogicalTypeID::UUID:
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
