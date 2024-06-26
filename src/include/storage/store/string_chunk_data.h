#pragma once

#include "common/assert.h"
#include "common/data_chunk/sel_vector.h"
#include "common/types/types.h"
#include "storage/store/dictionary_chunk.h"

namespace kuzu {
namespace storage {

class StringChunkData final : public ColumnChunkData {
public:
    StringChunkData(common::LogicalType dataType, uint64_t capacity, bool enableCompression,
        bool inMemory);

    void resetToEmpty() override;
    void append(common::ValueVector* vector, const common::SelectionVector& selVector) override;
    void append(ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;
    ColumnChunkData* getIndexColumnChunk();
    const ColumnChunkData* getIndexColumnChunk() const;

    void lookup(common::offset_t offsetInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const override;

    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets,
        common::RelMultiplicity multiplicity) override;
    void write(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
    void copy(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;

    template<typename T>
    T getValue(common::offset_t /*pos*/) const {
        KU_UNREACHABLE;
    }

    uint64_t getStringLength(common::offset_t pos) const {
        auto index = indexColumnChunk->getValue<DictionaryChunk::string_index_t>(pos);
        return dictionaryChunk->getStringLength(index);
    }

    DictionaryChunk& getDictionaryChunk() { return *dictionaryChunk; }
    const DictionaryChunk& getDictionaryChunk() const { return *dictionaryChunk; }

    void finalize() override;

    void resize(uint64_t newCapacity) override;

private:
    void appendStringColumnChunk(StringChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend);

    void setValueFromString(std::string_view value, uint64_t pos);

    void updateNumValues(size_t newValue);

    void setNumValues(uint64_t numValues) override {
        ColumnChunkData::setNumValues(numValues);
        indexColumnChunk->setNumValues(numValues);
    }

private:
    std::unique_ptr<ColumnChunkData> indexColumnChunk;

    std::unique_ptr<DictionaryChunk> dictionaryChunk;
    // If we never update a value, we don't need to prune unused strings in finalize
    bool needFinalize;
};

// STRING
template<>
std::string StringChunkData::getValue<std::string>(common::offset_t pos) const;
template<>
std::string_view StringChunkData::getValue<std::string_view>(common::offset_t pos) const;

} // namespace storage
} // namespace kuzu
