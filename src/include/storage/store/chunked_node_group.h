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
    explicit ChunkedNodeGroup(std::vector<std::unique_ptr<ColumnChunk>> chunks)
        : chunks{std::move(chunks)} {}
    ChunkedNodeGroup(const std::vector<common::LogicalType>& columnTypes, bool enableCompression,
        uint64_t capacity);
    ChunkedNodeGroup(const std::vector<std::unique_ptr<Column>>& columns, bool enableCompression);
    DELETE_COPY_DEFAULT_MOVE(ChunkedNodeGroup);
    virtual ~ChunkedNodeGroup() = default;

    inline uint64_t getNodeGroupIdx() const { return nodeGroupIdx; }
    inline common::vector_idx_t getNumColumns() const { return chunks.size(); }
    inline const ColumnChunk& getColumnChunk(common::column_id_t columnID) const {
        KU_ASSERT(columnID < chunks.size());
        return *chunks[columnID];
    }
    inline ColumnChunk& getColumnChunkUnsafe(common::column_id_t columnID) {
        KU_ASSERT(columnID < chunks.size());
        return *chunks[columnID];
    }
    inline std::vector<std::unique_ptr<ColumnChunk>>& getColumnChunksUnsafe() { return chunks; }
    inline bool isFull() const { return numRows == common::StorageConstants::NODE_GROUP_SIZE; }

    void resetToEmpty();
    void setAllNull();
    void setNumRows(common::offset_t numRows);
    inline common::row_idx_t getNumRows() const { return numRows; }
    void resizeChunks(uint64_t newSize);

    uint64_t append(const std::vector<common::ValueVector*>& columnVectors,
        common::SelectionVector& selVector, uint64_t numValuesToAppend);
    // Appends up to numValuesToAppend from the other chunked node group, returning the actual
    // number of values appended
    common::offset_t append(ChunkedNodeGroup* other, common::offset_t offsetInOtherNodeGroup,
        common::offset_t numValuesToAppend = common::StorageConstants::NODE_GROUP_SIZE);
    void write(const std::vector<std::unique_ptr<ColumnChunk>>& data,
        common::column_id_t offsetColumnID);
    void write(const ChunkedNodeGroup& data, common::column_id_t offsetColumnID);

    void finalize(uint64_t nodeGroupIdx_);

    virtual inline void writeToColumnChunk(common::vector_idx_t chunkIdx,
        common::vector_idx_t vectorIdx, const std::vector<std::unique_ptr<ColumnChunk>>& data,
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
    explicit ChunkedCSRHeader(bool enableCompression,
        uint64_t capacity = common::StorageConstants::NODE_GROUP_SIZE);
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
    ChunkedCSRNodeGroup(const std::vector<common::LogicalType>& columnTypes,
        bool enableCompression);
    DELETE_COPY_DEFAULT_MOVE(ChunkedCSRNodeGroup);

    ChunkedCSRHeader& getCSRHeader() { return csrHeader; }
    const ChunkedCSRHeader& getCSRHeader() const { return csrHeader; }

    inline void writeToColumnChunk(common::vector_idx_t chunkIdx, common::vector_idx_t vectorIdx,
        const std::vector<std::unique_ptr<ColumnChunk>>& data, ColumnChunk& offsetChunk) override {
        chunks[chunkIdx]->write(data[vectorIdx].get(), &offsetChunk, common::RelMultiplicity::MANY);
    }

private:
    ChunkedCSRHeader csrHeader;
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
