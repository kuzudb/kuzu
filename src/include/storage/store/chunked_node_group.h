#pragma once

#include "common/constants.h"
#include "common/enums/rel_multiplicity.h"
#include "storage/enums/residency_state.h"
#include "storage/store/column_chunk.h"
#include "storage/store/version_info.h"

namespace kuzu {
namespace common {
class SelectionVector;
} // namespace common

namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {

class Column;
struct TableScanState;
struct NodeGroupScanState;
class ChunkedNodeGroup {
public:
    static constexpr uint64_t CHUNK_CAPACITY = 2048;

    // TODO(Guodong): Seems like this should be removed.
    explicit ChunkedNodeGroup(std::vector<std::unique_ptr<ColumnChunk>> chunks)
        : ChunkedNodeGroup{std::move(chunks), common::INVALID_OFFSET, common::INVALID_ROW_IDX} {
        KU_ASSERT(!this->chunks.empty());
        residencyState = this->chunks[0]->getResidencyState();
        for (auto columnID = 1u; columnID < this->chunks.size(); columnID++) {
            KU_ASSERT(this->chunks[columnID]->getResidencyState() == residencyState);
        }
    }
    ChunkedNodeGroup(std::vector<std::unique_ptr<ColumnChunk>> chunks,
        common::offset_t startNodeOffset, common::row_idx_t startRowIdx)
        : chunks{std::move(chunks)}, startNodeOffset{startNodeOffset}, startRowIdx{startRowIdx} {
        KU_ASSERT(!this->chunks.empty());
        residencyState = this->chunks[0]->getResidencyState();
        numRows = this->chunks[0]->getNumValues();
        capacity = numRows;
        for (auto columnID = 1u; columnID < this->chunks.size(); columnID++) {
            KU_ASSERT(this->chunks[columnID]->getNumValues() == numRows);
            KU_ASSERT(this->chunks[columnID]->getResidencyState() == residencyState);
        }
    }
    ChunkedNodeGroup(const std::vector<common::LogicalType>& columnTypes, bool enableCompression,
        uint64_t capacity, common::offset_t startOffset, ResidencyState residencyState);
    // TODO(Guodong): Seems like this should be removed.
    ChunkedNodeGroup(const std::vector<std::unique_ptr<Column>>& columns, bool enableCompression);
    virtual ~ChunkedNodeGroup() = default;

    common::idx_t getNumColumns() const { return chunks.size(); }
    common::offset_t getStartNodeOffset() const { return startNodeOffset; }
    common::row_idx_t getStartRowIdx() const { return startRowIdx; }
    common::row_idx_t getNumRows() const { return numRows; }
    const ColumnChunk& getColumnChunk(const common::column_id_t columnID) const {
        KU_ASSERT(columnID < chunks.size());
        return *chunks[columnID];
    }
    ColumnChunk& getColumnChunk(const common::column_id_t columnID) {
        KU_ASSERT(columnID < chunks.size());
        return *chunks[columnID];
    }
    std::unique_ptr<ColumnChunk> moveColumnChunk(const common::column_id_t columnID) {
        KU_ASSERT(columnID < chunks.size());
        return std::move(chunks[columnID]);
    }
    bool isFullOrOnDisk() const {
        return numRows == capacity || residencyState == ResidencyState::ON_DISK;
    }
    ResidencyState getResidencyState() const { return residencyState; }

    void resetToEmpty();
    void setAllNull() const;
    void setNumRows(common::offset_t numRows_);
    void resizeChunks(uint64_t newSize) const;

    uint64_t append(const transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& columnVectors, common::SelectionVector& selVector,
        uint64_t numValuesToAppend);
    // Appends up to numValuesToAppend from the other chunked node group, returning the actual
    // number of values appended.
    common::offset_t append(const transaction::Transaction* transaction,
        const ChunkedNodeGroup& other, common::offset_t offsetInOtherNodeGroup,
        common::offset_t numValuesToAppend = common::StorageConstants::NODE_GROUP_SIZE);
    void write(const std::vector<std::unique_ptr<ColumnChunk>>& chunks,
        common::column_id_t offsetColumnID);
    void write(const ChunkedNodeGroup& data, common::column_id_t offsetColumnID);

    void scan(transaction::Transaction* transaction, TableScanState& scanState,
        NodeGroupScanState& nodeGroupScanState, common::offset_t offsetInGroup,
        common::length_t length) const;

    template<ResidencyState SCAN_RESIDENCY_STATE>
    void scanCommitted(transaction::Transaction* transaction, TableScanState& scanState,
        NodeGroupScanState& nodeGroupScanState, ChunkedNodeGroup& output) const;

    void lookup(transaction::Transaction* transaction, TableScanState& state,
        NodeGroupScanState& nodeGroupScanState, common::offset_t offsetInGroup) const;

    void update(transaction::Transaction* transaction, common::offset_t offset,
        common::column_id_t columnID, common::ValueVector& propertyVector);

    bool delete_(transaction::Transaction* transaction, common::offset_t offset);

    void finalize() const;

    virtual void writeToColumnChunk(common::idx_t chunkIdx, common::idx_t vectorIdx,
        const std::vector<std::unique_ptr<ColumnChunk>>& data, ColumnChunk& offsetChunk) {
        KU_ASSERT(residencyState != ResidencyState::ON_DISK);
        chunks[chunkIdx]->getData().write(&data[vectorIdx]->getData(), &offsetChunk.getData(),
            common::RelMultiplicity::ONE);
    }

    std::unique_ptr<ChunkedNodeGroup> flush(BMFileHandle& dataFH) const;

    uint64_t getEstimatedMemoryUsage() const;
    bool hasUpdates() const;

    virtual void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<ChunkedNodeGroup> deserialize(common::Deserializer& deSer);

protected:
    std::vector<std::unique_ptr<ColumnChunk>> chunks;

protected:
    ResidencyState residencyState;
    common::offset_t startNodeOffset;
    common::row_idx_t startRowIdx;
    uint64_t capacity;
    std::atomic<common::row_idx_t> numRows;
    std::unique_ptr<VersionInfo> versionInfo;
};

// struct NodeGroupFactory {
//     static std::unique_ptr<ChunkedNodeGroup> createNodeGroup(common::ColumnDataFormat dataFormat,
//         const std::vector<common::LogicalType>& columnTypes, bool enableCompression,
//         uint64_t capacity, ResidencyState residencyState) {
//         return dataFormat == common::ColumnDataFormat::REGULAR ?
//                    std::make_unique<ChunkedNodeGroup>(columnTypes, enableCompression, capacity,
//                        common::INVALID_OFFSET, residencyState) :
//                    std::make_unique<ChunkedCSRNodeGroup>(columnTypes, enableCompression,
//                    capacity,
//                        residencyState);
//     }
// };

} // namespace storage
} // namespace kuzu
