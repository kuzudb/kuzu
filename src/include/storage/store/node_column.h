#pragma once

#include "catalog/catalog.h"
#include "storage/copier/column_chunk.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/storage_structure.h"

namespace kuzu {
namespace storage {

using read_node_column_func_t = std::function<void(uint8_t* frame, PageElementCursor& pageCursor,
    common::ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead)>;
using write_node_column_func_t = std::function<void(
    uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVector)>;

struct ColumnChunkMetadata {
    common::page_idx_t pageIdx = common::INVALID_PAGE_IDX;
    common::page_idx_t numPages = 0;
};

struct FixedSizedNodeColumnFunc {
    static void readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead);
    static void writeValueToPage(
        uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVecto);

    static void readInternalIDValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead);
    static void writeInternalIDValueToPage(
        uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVecto);
};

struct NullNodeColumnFunc {
    static void readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead);
    static void writeValueToPage(
        uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVector);
};

struct BoolNodeColumnFunc {
    static void readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead);
    static void writeValueToPage(
        uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVector);
};

class NullNodeColumn;
// TODO(Guodong): This is intentionally duplicated with `Column`, as for now, we don't change rel
// tables. `Column` is used for rel tables only. Eventually, we should remove `Column`.
class NodeColumn {
public:
    NodeColumn(const catalog::Property& property, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, bool requireNullColumn = true);
    NodeColumn(common::LogicalType dataType, const catalog::MetadataDAHInfo& metaDAHeaderInfo,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        bool requireNullColumn);
    virtual ~NodeColumn() = default;

    // Expose for feature store
    virtual void batchLookup(const common::offset_t* nodeOffsets, size_t size, uint8_t* result);

    virtual void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);
    virtual void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0);
    virtual void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    virtual common::page_idx_t append(
        ColumnChunk* columnChunk, common::page_idx_t startPageIdx, uint64_t nodeGroupIdx);

    // TODO(Guodong): refactor these write interfaces.
    void write(common::ValueVector* nodeIDVector, common::ValueVector* vectorToWriteFrom);
    inline void write(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) {
        writeInternal(nodeOffset, vectorToWriteFrom, posInVectorToWriteFrom);
    }

    virtual void setNull(common::offset_t nodeOffset);

    inline common::LogicalType getDataType() const { return dataType; }
    inline uint32_t getNumBytesPerValue() const { return numBytesPerFixedSizedValue; }
    inline uint64_t getNumNodeGroups(transaction::Transaction* transaction) const {
        return metadataDA->getNumElements(transaction->getType());
    }

    virtual void checkpointInMemory();
    virtual void rollbackInMemory();

protected:
    virtual void scanInternal(transaction::Transaction* transaction,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    void scanUnfiltered(transaction::Transaction* transaction, PageElementCursor& pageCursor,
        uint64_t numValuesToScan, common::ValueVector* resultVector, uint64_t startPosInVector = 0);
    void scanFiltered(transaction::Transaction* transaction, PageElementCursor& pageCursor,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    virtual void lookupInternal(transaction::Transaction* transaction,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    virtual void lookupValue(transaction::Transaction* transaction, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t posInVector);

    void readFromPage(transaction::Transaction* transaction, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& func);

    virtual void writeInternal(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom);
    void writeValue(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom);

    // TODO(Guodong): This is mostly duplicated with StorageStructure::addNewPageToFileHandle().
    // Should be cleaned up later.
    void addNewPageToDataFH();
    // TODO(Guodong): This is mostly duplicated with
    // StorageStructure::createWALVersionOfPageIfNecessaryForElement(). Should be cleared later.
    WALPageIdxPosInPageAndFrame createWALVersionOfPageForValue(common::offset_t nodeOffset);

    static inline common::node_group_idx_t getNodeGroupIdxFromNodeOffset(
        common::offset_t nodeOffset) {
        return nodeOffset >> common::StorageConstants::NODE_GROUP_SIZE_LOG2;
    }

protected:
    StorageStructureID storageStructureID;
    common::LogicalType dataType;
    uint32_t numBytesPerFixedSizedValue;
    uint32_t numValuesPerPage;
    BMFileHandle* dataFH;
    BMFileHandle* metadataFH;
    BufferManager* bufferManager;
    WAL* wal;
    std::unique_ptr<InMemDiskArray<ColumnChunkMetadata>> metadataDA;
    std::unique_ptr<NodeColumn> nullColumn;
    std::vector<std::unique_ptr<NodeColumn>> childrenColumns;
    read_node_column_func_t readNodeColumnFunc;
    write_node_column_func_t writeNodeColumnFunc;
};

class BoolNodeColumn : public NodeColumn {
public:
    BoolNodeColumn(const catalog::MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        bool requireNullColumn = true);

    void batchLookup(const common::offset_t* nodeOffsets, size_t size, uint8_t* result) final;
};

class NullNodeColumn : public NodeColumn {
public:
    NullNodeColumn(common::page_idx_t metaDAHPageIdx, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal);

    void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    common::page_idx_t append(
        ColumnChunk* columnChunk, common::page_idx_t startPageIdx, uint64_t nodeGroupIdx) final;
    void setNull(common::offset_t nodeOffset) final;

protected:
    void writeInternal(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) final;
};

class SerialNodeColumn : public NodeColumn {
public:
    SerialNodeColumn(const catalog::MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal);

    void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    common::page_idx_t append(
        ColumnChunk* columnChunk, common::page_idx_t startPageIdx, uint64_t nodeGroupIdx) final;
};

struct NodeColumnFactory {
    static inline std::unique_ptr<NodeColumn> createNodeColumn(const catalog::Property& property,
        BMFileHandle* dataFH, BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal) {
        return createNodeColumn(*property.getDataType(), *property.getMetadataDAHInfo(), dataFH,
            metadataFH, bufferManager, wal);
    }
    static std::unique_ptr<NodeColumn> createNodeColumn(const common::LogicalType& dataType,
        const catalog::MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal);
};

} // namespace storage
} // namespace kuzu
