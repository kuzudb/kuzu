#pragma once

#include "common/column_data_format.h"
#include "common/copy_constructors.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

class Column;

class ChunkedNodeGroup {
public:
    ChunkedNodeGroup(const std::vector<common::LogicalType>& columnTypes, bool enableCompression,
        uint64_t capacity);
    ChunkedNodeGroup(const std::vector<std::unique_ptr<Column>>& columns, bool enableCompression);
    virtual ~ChunkedNodeGroup() = default;

    inline uint64_t getNodeGroupIdx() const { return nodeGroupIdx; }
    inline common::row_idx_t getNumRows() const { return numRows; }
    inline common::vector_idx_t getNumColumns() const { return chunks.size(); }
    inline const ColumnChunk& getColumnChunk(common::column_id_t columnID) const {
        KU_ASSERT(columnID < chunks.size());
        return *chunks[columnID];
    }
    inline ColumnChunk* getColumnChunkUnsafe(common::column_id_t columnID) {
        KU_ASSERT(columnID < chunks.size());
        return chunks[columnID].get();
    }
    inline const ColumnChunk& getColumnChunk(common::column_id_t columnID) {
        KU_ASSERT(columnID < chunks.size());
        return *chunks[columnID];
    }
    inline bool isFull() const { return numRows == common::StorageConstants::NODE_GROUP_SIZE; }

    void resetToEmpty();
    void setAllNull();
    void setNumValues(common::offset_t numValues);
    void resizeChunks(uint64_t newSize);

    uint64_t append(const std::vector<common::ValueVector*>& columnVectors,
        common::DataChunkState* columnState, uint64_t numValuesToAppend);
    common::offset_t append(ChunkedNodeGroup* other, common::offset_t offsetInOtherNodeGroup);
    void write(std::vector<std::unique_ptr<ColumnChunk>>& data, common::vector_idx_t offsetVector);

    void finalize(uint64_t nodeGroupIdx_);

    virtual inline void writeToColumnChunk(common::vector_idx_t chunkIdx,
        common::vector_idx_t vectorIdx, std::vector<std::unique_ptr<ColumnChunk>>& data,
        ColumnChunk& offsetChunk) {
        chunks[chunkIdx]->write(data[vectorIdx].get(), &offsetChunk, common::RelMultiplicity::ONE);
    }

protected:
    std::vector<std::unique_ptr<ColumnChunk>> chunks;

private:
    uint64_t nodeGroupIdx;
    common::row_idx_t numRows;
};

struct ChunkedCSRHeader {
    std::unique_ptr<ColumnChunk> offset;
    std::unique_ptr<ColumnChunk> length;

    ChunkedCSRHeader() {}
    explicit ChunkedCSRHeader(
        bool enableCompression, uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE);
    DELETE_COPY_DEFAULT_MOVE(ChunkedCSRHeader);

    common::offset_t getStartCSROffset(common::offset_t nodeOffset) const;
    common::offset_t getEndCSROffset(common::offset_t nodeOffset) const;
    common::length_t getCSRLength(common::offset_t nodeOffset) const;

    bool sanityCheck() const;
    void copyFrom(const ChunkedCSRHeader& other) const;
    void fillDefaultValues(common::offset_t newNumValues) const;
    inline void setNumValues(common::offset_t numValues) const {
        offset->setNumValues(numValues);
        length->setNumValues(numValues);
    }
};

class ChunkedCSRNodeGroup : public ChunkedNodeGroup {
public:
    ChunkedCSRNodeGroup(
        const std::vector<common::LogicalType>& columnTypes, bool enableCompression);

    ChunkedCSRHeader& getCSRHeader() { return csrHeader; }
    const ChunkedCSRHeader& getCSRHeader() const { return csrHeader; }

    inline void writeToColumnChunk(common::vector_idx_t chunkIdx, common::vector_idx_t vectorIdx,
        std::vector<std::unique_ptr<ColumnChunk>>& data, ColumnChunk& offsetChunk) override {
        chunks[chunkIdx]->write(data[vectorIdx].get(), &offsetChunk, common::RelMultiplicity::MANY);
    }

private:
    ChunkedCSRHeader csrHeader;
};

class ChunkedNodeGroupCollection {
public:
    ChunkedNodeGroupCollection() {}

    inline const std::vector<std::unique_ptr<ChunkedNodeGroup>>& getChunkedGroups() const {
        return chunkedGroups;
    }
    inline const ChunkedNodeGroup* getChunkedGroup(uint64_t groupIdx) const {
        KU_ASSERT(groupIdx < chunkedGroups.size());
        return chunkedGroups[groupIdx].get();
    }
    inline ChunkedNodeGroup* getChunkedGroupUnsafe(uint64_t groupIdx) {
        KU_ASSERT(groupIdx < chunkedGroups.size());
        return chunkedGroups[groupIdx].get();
    }
    inline uint64_t getNumChunks() const { return chunkedGroups.size(); }
    void append(std::unique_ptr<ChunkedNodeGroup> chunkedGroup);

private:
    // Assert that all chunked node groups have the same num columns and same data types.
    bool sanityCheckForAppend();

private:
    std::vector<std::unique_ptr<ChunkedNodeGroup>> chunkedGroups;
};

struct NodeGroupFactory {
    static inline std::unique_ptr<ChunkedNodeGroup> createNodeGroup(
        common::ColumnDataFormat dataFormat, const std::vector<common::LogicalType>& columnTypes,
        bool enableCompression, uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE) {
        return dataFormat == common::ColumnDataFormat::REGULAR ?
                   std::make_unique<ChunkedNodeGroup>(columnTypes, enableCompression, capacity) :
                   std::make_unique<ChunkedCSRNodeGroup>(columnTypes, enableCompression);
    }
};

} // namespace storage
} // namespace kuzu
