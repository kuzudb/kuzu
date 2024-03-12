#pragma once

#include "common/assert.h"
#include "common/data_chunk/sel_vector.h"
#include "common/types/types.h"
#include "storage/store/column_chunk.h"
#include "storage/store/dictionary_chunk.h"

namespace kuzu {
namespace storage {

class StringColumnChunk : public ColumnChunk {
public:
    StringColumnChunk(
        common::LogicalType dataType, uint64_t capacity, bool enableCompression, bool inMemory);

    void resetToEmpty() final;
    void append(common::ValueVector* vector, common::SelectionVector& selVector) final;
    void append(ColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) final;

    void write(common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) final;
    void write(ColumnChunk* chunk, ColumnChunk* dstOffsets, bool isCSR) final;
    void write(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
    void copy(ColumnChunk* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) final;

    template<typename T>
    T getValue(common::offset_t /*pos*/) const {
        KU_UNREACHABLE;
    }

    uint64_t getStringLength(common::offset_t pos) const {
        auto index = ColumnChunk::getValue<DictionaryChunk::string_index_t>(pos);
        return dictionaryChunk->getStringLength(index);
    }

    inline DictionaryChunk& getDictionaryChunk() { return *dictionaryChunk; }
    inline const DictionaryChunk& getDictionaryChunk() const { return *dictionaryChunk; }

    void finalize() final;

private:
    void appendStringColumnChunk(StringColumnChunk* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend);

    void setValueFromString(std::string_view value, uint64_t pos);

private:
    std::unique_ptr<DictionaryChunk> dictionaryChunk;
    // If we never update a value, we don't need to prune unused strings in finalize
    bool needFinalize;
};

// STRING
template<>
std::string StringColumnChunk::getValue<std::string>(common::offset_t pos) const;
template<>
std::string_view StringColumnChunk::getValue<std::string_view>(common::offset_t pos) const;

} // namespace storage
} // namespace kuzu
