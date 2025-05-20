#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>

#include "common/enums/rel_multiplicity.h"
#include "storage/enums/residency_state.h"
#include "storage/table/column_chunk.h"
#include "storage/table/column_chunk_data.h"
#include "storage/table/version_info.h"

namespace kuzu {
namespace common {
class SelectionVector;
} // namespace common

namespace transaction {
class Transaction;
} // namespace transaction

namespace storage {
class MemoryManager;

class Column;
struct TableScanState;
struct TableAddColumnState;
struct NodeGroupScanState;
class ColumnStats;
class FileHandle;

enum class NodeGroupDataFormat : uint8_t { REGULAR = 0, CSR = 1 };

class KUZU_API ChunkedNodeGroup {
public:
    ChunkedNodeGroup(std::vector<std::unique_ptr<ColumnChunk>> chunks,
        common::row_idx_t startRowIdx, NodeGroupDataFormat format = NodeGroupDataFormat::REGULAR);
    ChunkedNodeGroup(ChunkedNodeGroup& base,
        const std::vector<common::column_id_t>& selectedColumns);
    ChunkedNodeGroup(MemoryManager& mm, ChunkedNodeGroup& base,
        std::span<const common::LogicalType> columnTypes,
        std::span<const common::column_id_t> baseColumnIDs);
    ChunkedNodeGroup(MemoryManager& mm, const std::vector<common::LogicalType>& columnTypes,
        bool enableCompression, uint64_t capacity, common::row_idx_t startRowIdx,
        ResidencyState residencyState, NodeGroupDataFormat format = NodeGroupDataFormat::REGULAR);
    virtual ~ChunkedNodeGroup() = default;

    common::idx_t getNumColumns() const { return chunks.size(); }
    common::row_idx_t getStartRowIdx() const { return startRowIdx; }
    common::row_idx_t getNumRows() const { return numRows; }
    common::row_idx_t getCapacity() const { return capacity; }
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
    NodeGroupDataFormat getFormat() const { return format; }

    void merge(ChunkedNodeGroup& base, const std::vector<common::column_id_t>& columnsToMergeInto);
    void resetToEmpty();
    void resetToAllNull() const;
    void resetNumRowsFromChunks();
    void setNumRows(common::offset_t numRows_);
    void resizeChunks(uint64_t newSize);
    void setVersionInfo(std::unique_ptr<VersionInfo> versionInfo) {
        this->versionInfo = std::move(versionInfo);
    }
    void resetVersionAndUpdateInfo();

    uint64_t append(const transaction::Transaction* transaction,
        const std::vector<common::ValueVector*>& columnVectors, common::row_idx_t startRowInVectors,
        uint64_t numValuesToAppend);
    // Appends up to numValuesToAppend from the other chunked node group, returning the actual
    // number of values appended.
    common::offset_t append(const transaction::Transaction* transaction,
        const ChunkedNodeGroup& other, common::offset_t offsetInOtherNodeGroup,
        common::offset_t numRowsToAppend);
    common::offset_t append(const transaction::Transaction* transaction,
        const std::vector<common::column_id_t>& columnIDs, const ChunkedNodeGroup& other,
        common::offset_t offsetInOtherNodeGroup, common::offset_t numRowsToAppend);
    common::offset_t append(const transaction::Transaction* transaction,
        const std::vector<common::column_id_t>& columnIDs, const std::vector<ColumnChunk*>& other,
        common::offset_t offsetInOtherNodeGroup, common::offset_t numRowsToAppend);
    void write(const ChunkedNodeGroup& data, common::column_id_t offsetColumnID);

    void scan(const transaction::Transaction* transaction, const TableScanState& scanState,
        const NodeGroupScanState& nodeGroupScanState, common::offset_t rowIdxInGroup,
        common::length_t numRowsToScan) const;

    template<ResidencyState SCAN_RESIDENCY_STATE>
    void scanCommitted(transaction::Transaction* transaction, TableScanState& scanState,
        ChunkedNodeGroup& output) const;

    bool hasUpdates() const;
    bool hasDeletions(const transaction::Transaction* transaction) const;
    common::row_idx_t getNumUpdatedRows(const transaction::Transaction* transaction,
        common::column_id_t columnID);

    std::pair<std::unique_ptr<ColumnChunk>, std::unique_ptr<ColumnChunk>> scanUpdates(
        const transaction::Transaction* transaction, common::column_id_t columnID);

    bool lookup(const transaction::Transaction* transaction, const TableScanState& state,
        const NodeGroupScanState& nodeGroupScanState, common::offset_t rowIdxInChunk,
        common::sel_t posInOutput) const;

    void update(const transaction::Transaction* transaction, common::row_idx_t rowIdxInChunk,
        common::column_id_t columnID, const common::ValueVector& propertyVector);

    bool delete_(const transaction::Transaction* transaction, common::row_idx_t rowIdxInChunk);

    void addColumn(const transaction::Transaction* transaction,
        const TableAddColumnState& addColumnState, bool enableCompression, FileHandle* dataFH,
        ColumnStats* newColumnStats);

    bool isDeleted(const transaction::Transaction* transaction, common::row_idx_t rowInChunk) const;
    bool isInserted(const transaction::Transaction* transaction,
        common::row_idx_t rowInChunk) const;
    bool hasAnyUpdates(const transaction::Transaction* transaction, common::column_id_t columnID,
        common::row_idx_t startRow, common::length_t numRowsToCheck) const;
    common::row_idx_t getNumDeletions(const transaction::Transaction* transaction,
        common::row_idx_t startRow, common::length_t numRowsToCheck) const;
    bool hasVersionInfo() const { return versionInfo != nullptr; }

    void finalize() const;

    virtual void writeToColumnChunk(common::idx_t chunkIdx, common::idx_t vectorIdx,
        const std::vector<std::unique_ptr<ColumnChunk>>& data, ColumnChunk& offsetChunk) {
        KU_ASSERT(residencyState != ResidencyState::ON_DISK);
        chunks[chunkIdx]->getData().write(&data[vectorIdx]->getData(), &offsetChunk.getData(),
            common::RelMultiplicity::ONE);
    }

    virtual std::unique_ptr<ChunkedNodeGroup> flushAsNewChunkedNodeGroup(
        transaction::Transaction* transaction, FileHandle& dataFH) const;
    virtual void flush(FileHandle& dataFH);

    void commitInsert(common::row_idx_t startRow, common::row_idx_t numRowsToCommit,
        common::transaction_t commitTS);
    void rollbackInsert(common::row_idx_t startRow, common::row_idx_t numRows_,
        common::transaction_t commitTS);
    void commitDelete(common::row_idx_t startRow, common::row_idx_t numRows_,
        common::transaction_t commitTS);
    void rollbackDelete(common::row_idx_t startRow, common::row_idx_t numRows_,
        common::transaction_t commitTS);
    virtual void reclaimStorage(PageManager& pageManager) const;

    uint64_t getEstimatedMemoryUsage() const;

    virtual void serialize(common::Serializer& serializer) const;
    static std::unique_ptr<ChunkedNodeGroup> deserialize(MemoryManager& memoryManager,
        common::Deserializer& deSer);

    template<class TARGET>
    TARGET& cast() {
        return common::ku_dynamic_cast<TARGET&>(*this);
    }
    template<class TARGET>
    const TARGET& cast() const {
        return common::ku_dynamic_cast<const TARGET&>(*this);
    }

    // Also marks the chunks as in-use
    // I.e. if you want to be able to spill to disk again you must call setUnused first
    void loadFromDisk(const MemoryManager& mm);

    // returns the amount of space reclaimed in bytes
    uint64_t spillToDisk();

    void setUnused(const MemoryManager& mm);

private:
    void handleAppendException();

protected:
    NodeGroupDataFormat format;
    ResidencyState residencyState;
    common::row_idx_t startRowIdx;
    uint64_t capacity;
    std::atomic<common::row_idx_t> numRows;
    std::vector<std::unique_ptr<ColumnChunk>> chunks;
    std::unique_ptr<VersionInfo> versionInfo;
    std::mutex spillToDiskMutex;
    // Used to track if the group may be in use and to verify that spillToDisk is only called when
    // it is safe to do so. If false, it is safe to spill the data to disk.
    bool dataInUse;
};

} // namespace storage
} // namespace kuzu
