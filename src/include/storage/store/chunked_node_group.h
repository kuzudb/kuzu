#pragma once

#include "common/column_data_format.h"
#include "common/constants.h"
#include "common/copy_constructors.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class Column;

class ChunkedNodeGroup {
public:
    // TODO: Figure out a correct consutrctor to pass in startNodeOffset.
    explicit ChunkedNodeGroup(std::vector<std::unique_ptr<ColumnChunk>> chunks)
        : ChunkedNodeGroup{std::move(chunks), common::INVALID_OFFSET} {}
    explicit ChunkedNodeGroup(std::vector<std::unique_ptr<ColumnChunk>> chunks,
        common::offset_t startNodeOffset)
        : chunks{std::move(chunks)}, nodeGroupIdx{common::INVALID_NODE_GROUP_IDX},
          startNodeOffset{startNodeOffset}, capacity{common::StorageConstants::NODE_GROUP_SIZE} {
        numRows = this->chunks.empty() ? 0 : this->chunks[0]->getNumValues();
        for (auto columnID = 1; columnID < this->chunks.size(); columnID++) {
            KU_ASSERT(this->chunks[columnID]->getNumValues() == numRows);
        }
    }
    ChunkedNodeGroup(const std::vector<common::LogicalType>& columnTypes, bool enableCompression,
        uint64_t capacity, common::offset_t startOffset);
    ChunkedNodeGroup(const std::vector<std::unique_ptr<Column>>& columns, bool enableCompression);
    DELETE_COPY_DEFAULT_MOVE(ChunkedNodeGroup);
    virtual ~ChunkedNodeGroup() = default;

    // TODO(Guodong): Currently we rely on this field in `append` to figure out node group idx to
    // append. Should be removed after we move `append` logic to NodeGroupCollection, which can
    // natraully figure out node group idx.
    uint64_t getNodeGroupIdx() const { return nodeGroupIdx; }
    common::offset_t getStartNodeOffset() const { return startNodeOffset; }
    common::vector_idx_t getNumColumns() const { return chunks.size(); }
    const ColumnChunk& getColumnChunk(common::column_id_t columnID) const {
        KU_ASSERT(columnID < chunks.size());
        return *chunks[columnID];
    }
    ColumnChunk& getColumnChunkUnsafe(common::column_id_t columnID) const {
        KU_ASSERT(columnID < chunks.size());
        return *chunks[columnID];
    }
    std::vector<std::unique_ptr<ColumnChunk>>& getColumnChunksUnsafe() { return chunks; }
    bool isFull() const { return numRows == capacity; }

    void resetToEmpty();
    void setAllNull();
    void setNumRows(common::offset_t numRows);
    common::row_idx_t getNumRows() const { return numRows; }
    void resizeChunks(uint64_t newSize);

    uint64_t append(const std::vector<common::ValueVector*>& columnVectors,
        common::SelectionVector& selVector, uint64_t numValuesToAppend);
    // Appends up to numValuesToAppend from the other chunked node group, returning the actual
    // number of values appended.
    common::offset_t append(const ChunkedNodeGroup& other, common::offset_t offsetInOtherNodeGroup,
        common::offset_t numValuesToAppend = common::StorageConstants::NODE_GROUP_SIZE);
    void write(const std::vector<std::unique_ptr<ColumnChunk>>& data,
        common::column_id_t offsetColumnID);
    void write(const ChunkedNodeGroup& data, common::column_id_t offsetColumnID);

    void scan(const std::vector<common::column_id_t>& columnIDs,
        const std::vector<common::ValueVector*>& outputVectors, common::offset_t offset,
        common::length_t length) const;

    void finalize(uint64_t nodeGroupIdx_);

    virtual void writeToColumnChunk(common::vector_idx_t chunkIdx, common::vector_idx_t vectorIdx,
        const std::vector<std::unique_ptr<ColumnChunk>>& data, ColumnChunk& offsetChunk) {
        chunks[chunkIdx]->write(data[vectorIdx].get(), &offsetChunk, common::RelMultiplicity::ONE);
    }

    std::unique_ptr<ChunkedNodeGroup> flush(BMFileHandle& dataFH) const;

protected:
    std::vector<std::unique_ptr<ColumnChunk>> chunks;

private:
    // TODO: This should be removed. See comment on `getNodeGroupIdx()`. Instead, should only keep
    // `startNodeOffset`.
    uint64_t nodeGroupIdx;
    common::offset_t startNodeOffset;
    uint64_t capacity;
    common::row_idx_t numRows;
};

struct ChunkedCSRHeader {
    std::unique_ptr<ColumnChunk> offset;
    std::unique_ptr<ColumnChunk> length;

    ChunkedCSRHeader() {}
    explicit ChunkedCSRHeader(bool enableCompression,
        uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE);
    DELETE_COPY_DEFAULT_MOVE(ChunkedCSRHeader);

    common::offset_t getStartCSROffset(common::offset_t nodeOffset) const;
    common::offset_t getEndCSROffset(common::offset_t nodeOffset) const;
    common::length_t getCSRLength(common::offset_t nodeOffset) const;

    bool sanityCheck() const;
    void copyFrom(const ChunkedCSRHeader& other) const;
    void fillDefaultValues(common::offset_t newNumValues) const;
    void setNumValues(common::offset_t numValues) const {
        offset->setNumValues(numValues);
        length->setNumValues(numValues);
    }
};

class ChunkedCSRNodeGroup final : public ChunkedNodeGroup {
public:
    ChunkedCSRNodeGroup(const std::vector<common::LogicalType>& columnTypes,
        bool enableCompression);
    DELETE_COPY_DEFAULT_MOVE(ChunkedCSRNodeGroup);

    ChunkedCSRHeader& getCSRHeader() { return csrHeader; }
    const ChunkedCSRHeader& getCSRHeader() const { return csrHeader; }

    void writeToColumnChunk(common::vector_idx_t chunkIdx, common::vector_idx_t vectorIdx,
        const std::vector<std::unique_ptr<ColumnChunk>>& data, ColumnChunk& offsetChunk) override {
        chunks[chunkIdx]->write(data[vectorIdx].get(), &offsetChunk, common::RelMultiplicity::MANY);
    }

private:
    ChunkedCSRHeader csrHeader;
};

struct NodeGroupFactory {
    static std::unique_ptr<ChunkedNodeGroup> createNodeGroup(common::ColumnDataFormat dataFormat,
        const std::vector<common::LogicalType>& columnTypes, bool enableCompression,
        uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE) {
        return dataFormat == common::ColumnDataFormat::REGULAR ?
                   std::make_unique<ChunkedNodeGroup>(columnTypes, enableCompression, capacity,
                       common::INVALID_OFFSET) :
                   std::make_unique<ChunkedCSRNodeGroup>(columnTypes, enableCompression);
    }
};

} // namespace storage
} // namespace kuzu
