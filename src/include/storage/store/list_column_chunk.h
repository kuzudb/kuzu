#pragma once

#include "common/data_chunk/sel_vector.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

// TODO(Guodong): Let's simplify the data structure here by getting rid of this class.
struct ListDataColumnChunk {
    std::unique_ptr<ColumnChunkData> dataColumnChunk;
    uint64_t capacity;

    explicit ListDataColumnChunk(std::unique_ptr<ColumnChunkData> dataChunk)
        : dataColumnChunk{std::move(dataChunk)}, capacity{this->dataColumnChunk->capacity} {}

    void reset() const;

    void resizeBuffer(uint64_t numValues);

    void append(common::ValueVector* dataVector, const common::SelectionVector& selVector) const {
        dataColumnChunk->append(dataVector, selVector);
    }

    uint64_t getNumValues() const { return dataColumnChunk->getNumValues(); }
};

class ListChunkData final : public ColumnChunkData {

public:
    ListChunkData(common::LogicalType dataType, uint64_t capacity, bool enableCompression,
        bool inMemory);

    ColumnChunkData* getDataColumnChunk() const {
        return listDataColumnChunk->dataColumnChunk.get();
    }

    ColumnChunkData* getSizeColumnChunk() const { return sizeColumnChunk.get(); }

    void resetToEmpty() override;

    void setNumValues(uint64_t numValues_) override {
        ColumnChunkData::setNumValues(numValues_);
        sizeColumnChunk->setNumValues(numValues_);
    }

    void append(common::ValueVector* vector, const common::SelectionVector& selVector) final;

    void lookup(common::offset_t offsetInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const override;

    // Note: `write` assumes that no `append` will be called afterward.
    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets,
        common::RelMultiplicity multiplicity) override;
    void write(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
    void copy(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;

    void resizeDataColumnChunk(uint64_t numValues) { listDataColumnChunk->resizeBuffer(numValues); }

    void resize(uint64_t newCapacity) override {
        ColumnChunkData::resize(newCapacity);
        sizeColumnChunk->resize(newCapacity);
    }

    common::offset_t getListStartOffset(common::offset_t offset) const;

    common::offset_t getListEndOffset(common::offset_t offset) const;

    common::list_size_t getListSize(common::offset_t offset) const;

    void resetOffset();
    void resetFromOtherChunk(ListChunkData* other);
    void finalize() override;
    bool isOffsetsConsecutiveAndSortedAscending(uint64_t startPos, uint64_t endPos) const;
    bool sanityCheck() override;

protected:
    void copyListValues(const common::list_entry_t& entry, common::ValueVector* dataVector);

private:
    void append(ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;

    void appendNullList();

protected:
    std::unique_ptr<ColumnChunkData> sizeColumnChunk;
    std::unique_ptr<ListDataColumnChunk> listDataColumnChunk;
    // we use checkOffsetSortedAsc flag to indicate that we do not trigger random write
    bool checkOffsetSortedAsc;
};

} // namespace storage
} // namespace kuzu
