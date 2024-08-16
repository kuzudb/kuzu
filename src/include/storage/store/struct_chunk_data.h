#pragma once

#include "common/data_chunk/sel_vector.h"
#include "common/types/types.h"
#include "storage/store/column_chunk_data.h"

namespace kuzu {
namespace storage {

class StructChunkData final : public ColumnChunkData {
public:
    StructChunkData(common::LogicalType dataType, uint64_t capacity, bool enableCompression,
        ResidencyState residencyState);
    StructChunkData(common::LogicalType dataType, bool enableCompression,
        const ColumnChunkMetadata& metadata);

    ColumnChunkData* getChild(common::idx_t childIdx) {
        KU_ASSERT(childIdx < childChunks.size());
        return childChunks[childIdx].get();
    }
    std::unique_ptr<ColumnChunkData> moveChild(common::idx_t childIdx) {
        KU_ASSERT(childIdx < childChunks.size());
        return std::move(childChunks[childIdx]);
    }

    void finalize() override;

    uint64_t getEstimatedMemoryUsage() const override;

    uint64_t getNumValues() const override { return nullData->getNumValues(); }
    void resetNumValuesFromMetadata() override;
    void syncNumValues() override {
        KU_ASSERT(!childChunks.empty());
        numValues = childChunks[0]->getNumValues();
        metadata.numValues = numValues;
    }

    void serialize(common::Serializer& serializer) const override;
    static void deserialize(common::Deserializer& deSer, ColumnChunkData& chunkData);

    common::idx_t getNumChildren() const { return childChunks.size(); }
    const ColumnChunkData& getChild(common::idx_t childIdx) const {
        KU_ASSERT(childIdx < childChunks.size());
        return *childChunks[childIdx];
    }
    void setChild(common::idx_t childIdx, std::unique_ptr<ColumnChunkData> childChunk) {
        KU_ASSERT(childIdx < childChunks.size());
        childChunks[childIdx] = std::move(childChunk);
    }

    void flush(BMFileHandle& dataFH) override;

protected:
    void append(ColumnChunkData* other, common::offset_t startPosInOtherChunk,
        uint32_t numValuesToAppend) override;
    void append(common::ValueVector* vector, const common::SelectionVector& selVector) override;

    void scan(common::ValueVector& output, common::offset_t offset, common::length_t length,
        common::sel_t posInOutputVector = 0) const override;
    void lookup(common::offset_t offsetInChunk, common::ValueVector& output,
        common::sel_t posInOutputVector) const override;
    void initializeScanState(ChunkState& state) const override;

    void write(const common::ValueVector* vector, common::offset_t offsetInVector,
        common::offset_t offsetInChunk) override;
    void write(ColumnChunkData* chunk, ColumnChunkData* dstOffsets,
        common::RelMultiplicity multiplicity) override;
    void write(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;
    void copy(ColumnChunkData* srcChunk, common::offset_t srcOffsetInChunk,
        common::offset_t dstOffsetInChunk, common::offset_t numValuesToCopy) override;

    void setToInMemory() override;
    void resize(uint64_t newCapacity) override;

    void resetToEmpty() override;
    void resetToAllNull() override;

    bool numValuesSanityCheck() const override;

    void setNumValues(uint64_t numValues) override {
        ColumnChunkData::setNumValues(numValues);
        for (auto& childChunk : childChunks) {
            childChunk->setNumValues(numValues);
        }
    }

private:
    std::vector<std::unique_ptr<ColumnChunkData>> childChunks;
};

} // namespace storage
} // namespace kuzu
