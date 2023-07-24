#pragma once

#include "catalog/catalog.h"
#include "storage/storage_structure/disk_array.h"
#include "storage/storage_structure/storage_structure.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

using node_group_idx_t = uint64_t;

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
    static void writeValuesToPage(
        uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVecto);

    static void readInternalIDValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead);
    static void writeInternalIDValuesToPage(
        uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVecto);
};

struct NullNodeColumnFunc {
    static void readValuesFromPage(uint8_t* frame, PageElementCursor& pageCursor,
        common::ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead);
    static void writeValuesToPage(
        uint8_t* frame, uint16_t posInFrame, common::ValueVector* vector, uint32_t posInVector);
};

class NullNodeColumn;
// TODO(Guodong): This is intentionally duplicated with `Column`, as for now, we don't change rel
// tables. `Column` is used for rel tables only. Eventually, we should remove `Column`.
class NodeColumn {
public:
    NodeColumn(const catalog::Property& property, BMFileHandle* nodeGroupsDataFH,
        BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager, WAL* wal,
        bool requireNullColumn = true);
    NodeColumn(common::LogicalType dataType,
        const catalog::MetaDiskArrayHeaderInfo& metaDAHeaderInfo, BMFileHandle* nodeGroupsDataFH,
        BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager, WAL* wal,
        bool requireNullColumn);
    virtual ~NodeColumn() = default;

    // Expose for feature store
    void batchLookup(const common::offset_t* nodeOffsets, size_t size, uint8_t* result);

    virtual void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);
    virtual void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector);

    virtual common::page_idx_t appendColumnChunk(
        ColumnChunk* columnChunk, common::page_idx_t startPageIdx, uint64_t nodeGroupIdx);

    virtual void write(common::ValueVector* nodeIDVector, common::ValueVector* vectorToWriteFrom);

    virtual void setNull(common::offset_t nodeOffset);

    inline uint32_t getNumBytesPerValue() const { return numBytesPerFixedSizedValue; }
    inline uint64_t getNumNodeGroups(transaction::Transaction* transaction) const {
        return columnChunksMetaDA->getNumElements(transaction->getType());
    }

    void checkpointInMemory();
    void rollbackInMemory();

protected:
    virtual void scanInternal(transaction::Transaction* transaction,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    void scanUnfiltered(transaction::Transaction* transaction, PageElementCursor& pageCursor,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    void scanFiltered(transaction::Transaction* transaction, PageElementCursor& pageCursor,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    virtual void lookupInternal(transaction::Transaction* transaction,
        common::ValueVector* nodeIDVector, common::ValueVector* resultVector);
    void lookupSingleValue(transaction::Transaction* transaction, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t posInVector);

    void readFromPage(transaction::Transaction* transaction, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& func);

    virtual void writeInternal(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom);
    void writeSingleValue(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom);

    // TODO(Guodong): This is mostly duplicated with StorageStructure::addNewPageToFileHandle().
    // Should be cleaned up later.
    void addNewPageToNodeGroupsDataFH();
    // TODO(Guodong): This is mostly duplicated with
    // StorageStructure::createWALVersionOfPageIfNecessaryForElement(). Should be cleared later.
    WALPageIdxPosInPageAndFrame createWALVersionOfPageForValue(common::offset_t nodeOffset);

    static inline node_group_idx_t getNodeGroupIdxFromNodeOffset(common::offset_t nodeOffset) {
        return nodeOffset >> common::StorageConstants::NODE_GROUP_SIZE_LOG2;
    }

protected:
    StorageStructureID storageStructureID;
    common::LogicalType dataType;
    uint32_t numBytesPerFixedSizedValue;
    uint32_t numValuesPerPage;
    BMFileHandle* nodeGroupsDataFH;
    BufferManager* bufferManager;
    WAL* wal;
    std::unique_ptr<InMemDiskArray<ColumnChunkMetadata>> columnChunksMetaDA;
    std::unique_ptr<NodeColumn> nullColumn;
    std::vector<std::unique_ptr<NodeColumn>> childrenColumns;
    read_node_column_func_t readNodeColumnFunc;
    write_node_column_func_t writeNodeColumnFunc;
};

class NullNodeColumn : public NodeColumn {
public:
    NullNodeColumn(common::page_idx_t metaDAHeaderPageIdx, BMFileHandle* nodeGroupsDataFH,
        BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager, WAL* wal);

    void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    common::page_idx_t appendColumnChunk(
        ColumnChunk* columnChunk, common::page_idx_t startPageIdx, uint64_t nodeGroupIdx) final;
    void setNull(common::offset_t nodeOffset) final;

protected:
    void writeInternal(common::offset_t nodeOffset, common::ValueVector* vectorToWriteFrom,
        uint32_t posInVectorToWriteFrom) final;
};

class SerialNodeColumn : public NodeColumn {
public:
    SerialNodeColumn(const catalog::MetaDiskArrayHeaderInfo& metaDAHeaderInfo,
        BMFileHandle* nodeGroupsDataFH, BMFileHandle* nodeGroupsMetaFH,
        BufferManager* bufferManager, WAL* wal);

    void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;
    common::page_idx_t appendColumnChunk(
        ColumnChunk* columnChunk, common::page_idx_t startPageIdx, uint64_t nodeGroupIdx) final;
};

struct NodeColumnFactory {
    static inline std::unique_ptr<NodeColumn> createNodeColumn(const catalog::Property& property,
        BMFileHandle* nodeGroupsDataFH, BMFileHandle* nodeGroupsMetaFH,
        BufferManager* bufferManager, WAL* wal) {
        return createNodeColumn(property.dataType, property.metaDiskArrayHeaderInfo,
            nodeGroupsDataFH, nodeGroupsMetaFH, bufferManager, wal);
    }
    static std::unique_ptr<NodeColumn> createNodeColumn(const common::LogicalType& dataType,
        const catalog::MetaDiskArrayHeaderInfo& metaDAHeaderInfo, BMFileHandle* nodeGroupsDataFH,
        BMFileHandle* nodeGroupsMetaFH, BufferManager* bufferManager, WAL* wal);
};

} // namespace storage
} // namespace kuzu
