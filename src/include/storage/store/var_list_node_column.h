#pragma once

#include "node_column.h"

// List is a nested data type which is stored as two columns:
// 1. Offset column (type: INT64). Using offset to partition the data column into multiple lists.
// 2. Data column. Stores the actual data of the list.
// Similar to other data types, nulls are stored in the null column.
// Example layout for list of INT64:
// Four lists: [4,7,8,12], null, [2, 3], []
// Offset column: [4, 4, 6, 6]
// data column: [4, 7, 8, 12, 2, 3]
// When reading the data, we firstly read the offset column and utilize the offset column to find
// the data column partitions.
// 1st list(offset 4): Since it is the first element, the start offset is a constant 0, and the end
// offset is 4. Its data is stored at position 0-4 in the data column.
// 2nd list(offset 4): By reading the null column, we know that the 2nd list is null. So we don't
// need to read from the data column.
// 3rd list(offset 6): The start offset is 4(by looking up the offset for the 2nd list), and the end
// offset is 6. Its data is stored at position 4-6 in the data column.
// 4th list(offset 6): The start offset is 6(by looking up the offset for the 3rd list), and the end
// offset is 6. Its data is stored at position 6-6 in the data column (empty list).

namespace kuzu {
namespace storage {

struct ListOffsetInfoInStorage {
    common::offset_t prevNodeListOffset;
    std::unique_ptr<common::ValueVector> offsetVector;

    ListOffsetInfoInStorage(
        common::offset_t prevNodeListOffset, std::unique_ptr<common::ValueVector> offsetVector)
        : prevNodeListOffset{prevNodeListOffset}, offsetVector{std::move(offsetVector)} {}

    inline common::offset_t getListOffset(uint64_t nodePos) const {
        return nodePos == 0 ? prevNodeListOffset :
                              offsetVector->getValue<common::offset_t>(nodePos - 1);
    }

    inline uint64_t getListLength(uint64_t nodePos) const {
        return getListOffset(nodePos + 1) - getListOffset(nodePos);
    }
};

class VarListNodeColumn : public NodeColumn {
public:
    VarListNodeColumn(common::LogicalType dataType,
        const catalog::MetadataDAHInfo& metaDAHeaderInfo, BMFileHandle* dataFH,
        BMFileHandle* metadataFH, BufferManager* bufferManager, WAL* wal,
        transaction::Transaction* transaction)
        : NodeColumn{std::move(dataType), metaDAHeaderInfo, dataFH, metadataFH, bufferManager, wal,
              transaction, true /* requireNullColumn */} {
        dataNodeColumn = NodeColumnFactory::createNodeColumn(
            *common::VarListType::getChildType(&this->dataType), *metaDAHeaderInfo.childrenInfos[0],
            dataFH, metadataFH, bufferManager, wal, transaction);
    }

    void scan(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::offset_t startOffsetInGroup, common::offset_t endOffsetInGroup,
        common::ValueVector* resultVector, uint64_t offsetInVector = 0) final;

protected:
    void scanInternal(transaction::Transaction* transaction, common::ValueVector* nodeIDVector,
        common::ValueVector* resultVector) final;

    void lookupValue(transaction::Transaction* transaction, common::offset_t nodeOffset,
        common::ValueVector* resultVector, uint32_t posInVector) final;

    common::page_idx_t append(
        ColumnChunk* columnChunk, common::page_idx_t startPageIdx, uint64_t nodeGroupIdx) override;

private:
    inline common::offset_t readListOffsetInStorage(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::offset_t offsetInNodeGroup) {
        return offsetInNodeGroup == 0 ?
                   0 :
                   readOffset(transaction, nodeGroupIdx, offsetInNodeGroup - 1);
    }

    void scanUnfiltered(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::ValueVector* resultVector,
        const ListOffsetInfoInStorage& listOffsetInfoInStorage);

    void scanFiltered(transaction::Transaction* transaction, common::node_group_idx_t nodeGroupIdx,
        common::ValueVector* offsetVector, const ListOffsetInfoInStorage& listOffsetInfoInStorage);

    void checkpointInMemory() final;

    void rollbackInMemory() final;

    common::offset_t readOffset(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::offset_t offsetInNodeGroup);

    ListOffsetInfoInStorage getListOffsetInfoInStorage(transaction::Transaction* transaction,
        common::node_group_idx_t nodeGroupIdx, common::offset_t startOffsetInNodeGroup,
        common::offset_t endOffsetInNodeGroup, std::shared_ptr<common::DataChunkState> state);

private:
    std::unique_ptr<NodeColumn> dataNodeColumn;
};

} // namespace storage
} // namespace kuzu
