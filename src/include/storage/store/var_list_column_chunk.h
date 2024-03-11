#pragma once

#include "common/data_chunk/sel_vector.h"
#include "storage/store/column_chunk.h"

namespace kuzu {
namespace storage {

// TODO(Guodong): Let's simplify the data structure here by getting rid of this class.
struct VarListDataColumnChunk {
    std::unique_ptr<ColumnChunk> dataColumnChunk;
    uint64_t capacity;

    explicit VarListDataColumnChunk(std::unique_ptr<ColumnChunk> dataChunk)
        : dataColumnChunk{std::move(dataChunk)}, capacity{this->dataColumnChunk->capacity} {}

    void reset() const;

    void resizeBuffer(uint64_t numValues);

    inline void append(common::ValueVector* dataVector, common::SelectionVector& selVector) const {
        dataColumnChunk->append(dataVector, selVector);
    }

    inline uint64_t getNumValues() const { return dataColumnChunk->getNumValues(); }
};

class VarListColumnChunk : public ColumnChunk {

public:
    VarListColumnChunk(
        common::LogicalType dataType, uint64_t capacity, bool enableCompression, bool inMemory);

    inline ColumnChunk* getDataColumnChunk() const {
        return varListDataColumnChunk->dataColumnChunk.get();
    }

    inline ColumnChunk* getSizeColumnChunk() const { return sizeColumnChunk.get(); }

    void resetToEmpty() final;

    inline void setNumValues(uint64_t numValues_) final {
        ColumnChunk::setNumValues(numValues_);
        sizeColumnChunk->setNumValues(numValues_);
    }

    void append(common::ValueVector* vector, common::SelectionVector& selVector) final;
    // Note: `write` assumes that no `append` will be called afterward.
    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) final;
    void write(ColumnChunk* chunk, ColumnChunk* dstOffsets, bool isCSR) final;
    void write(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
    void copy(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) final;

    inline void resizeDataColumnChunk(uint64_t numValues) {
        varListDataColumnChunk->resizeBuffer(numValues);
    }

    inline void resize(uint64_t newCapacity) final {
        ColumnChunk::resize(newCapacity);
        sizeColumnChunk->resize(newCapacity);
    }

    void finalize() final;

    inline common::offset_t getListStartOffset(common::offset_t offset) const {
        if (numValues == 0)
            return 0;
        return offset == numValues ? getListEndOffset(offset - 1) :
                                     getListEndOffset(offset) - getListLen(offset);
    }

    inline common::offset_t getListEndOffset(common::offset_t offset) const {
        KU_ASSERT(offset < numValues);
        return getValue<uint64_t>(offset);
    }

    inline uint64_t getListLen(common::offset_t offset) const {
        KU_ASSERT(offset < sizeColumnChunk->getNumValues());
        return sizeColumnChunk->getValue<uint64_t>(offset);
    }

    void resetOffset();

protected:
    void copyListValues(const common::list_entry_t& entry, common::ValueVector* dataVector);

private:
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) final;

    inline void initializeIndices() {
        indicesColumnChunk = ColumnChunkFactory::createColumnChunk(
            *common::LogicalType::INT64(), false /*enableCompression*/, capacity);
        indicesColumnChunk->getNullChunk()->resetToAllNull();
        for (auto i = 0u; i < numValues; i++) {
            indicesColumnChunk->setValue<common::offset_t>(i, i);
            indicesColumnChunk->getNullChunk()->setNull(i, nullChunk->isNull(i));
        }
        indicesColumnChunk->setNumValues(numValues);
    }

    void resetFromOtherChunk(VarListColumnChunk* other);
    void appendNullList();

protected:
    std::unique_ptr<ColumnChunk> sizeColumnChunk;
    std::unique_ptr<VarListDataColumnChunk> varListDataColumnChunk;
    // The following is needed to write var list to random positions in the column chunk.
    // We first append var list to the end of the column chunk. Then use indicesColumnChunk to track
    // where each var list data is inside the column chunk.
    // `needFinalize` is set to true whenever `write` is called.
    // During `finalize`, the whole column chunk will be re-written according to indices.
    bool needFinalize;
    std::unique_ptr<ColumnChunk> indicesColumnChunk;
};

} // namespace storage
} // namespace kuzu
