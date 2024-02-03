#pragma once
#include <iostream>

#include "storage/store/struct_column.h"
using namespace kuzu::common;

namespace kuzu {
namespace storage {

class InternalIDColumn : public StructColumn {
public:
    InternalIDColumn(std::string name, std::unique_ptr<LogicalType> dataType,
        const MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH, BMFileHandle* metadataFH,
        BufferManager* bufferManager, WAL* wal, transaction::Transaction* transaction,
        RWPropertyStats stats, bool enableCompression)
        : StructColumn(name, std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH,
              bufferManager, wal, transaction, stats, enableCompression){};

    void append(ColumnChunk* columnChunk, uint64_t nodeGroupIdx) override;

    // TODO: need to rewrite; we do not finalize StructColumn pagenums
    inline InMemDiskArray<ColumnChunkMetadata>* getMetadataDA() const override {
        return childColumns[1]->metadataDA.get();
    }

    inline ColumnChunkMetadata getMetadata(common::node_group_idx_t nodeGroupIdx,
        transaction::TransactionType transaction) const override {
        return childColumns[1]->metadataDA->get(nodeGroupIdx, transaction);
    }

    ReadState getReadState(transaction::TransactionType transactionType,
        common::node_group_idx_t nodeGroupIdx) const override {
        auto metadata = childColumns[1]->metadataDA->get(nodeGroupIdx, transactionType);
        return {
            metadata, metadata.compMeta.numValues(BufferPoolConstants::PAGE_4KB_SIZE, *dataType)};
    }

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        ColumnChunk* columnChunk, common::offset_t startOffset = 0,
        common::offset_t endOffset = common::INVALID_OFFSET) override;

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector) override;

    void scan(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) override;

    void lookup(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) override;

    void scanInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) override;

    void write(common::node_group_idx_t nodeGroupIdx, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom) override {
        KU_ASSERT(vectorToWriteFrom->dataType.getPhysicalType() == PhysicalTypeID::STRUCT);
        StructColumn::write(nodeGroupIdx, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom);
    }
    static std::shared_ptr<ValueVector> reWriteAsStructIDVector(
        ValueVector* resultVector, MemoryManager* mm);
};

} // namespace storage
} // namespace kuzu
