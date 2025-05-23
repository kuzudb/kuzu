#pragma once

#include "storage/compression/float_compression.h"

namespace kuzu {
namespace transaction {
class Transaction;
}
namespace storage {

class FileHandle;
class ColumnReadWriter;
class ShadowFile;
struct ColumnChunkMetadata;
struct ChunkState;

template<typename OutputType>
using read_value_from_page_func_t = std::function<void(uint8_t*, PageCursor&, OutputType, uint32_t,
    uint64_t, const CompressionMetadata&)>;

template<typename OutputType>
using read_values_from_page_func_t = std::function<void(uint8_t*, PageCursor&, OutputType, uint32_t,
    uint64_t, const CompressionMetadata&)>;

using read_values_to_vector_func_t = read_values_from_page_func_t<common::ValueVector*>;
using read_values_to_page_func_t = read_values_from_page_func_t<uint8_t*>;

template<typename InputType, typename... AdditionalArgs>
using write_values_to_page_func_t = std::function<void(uint8_t*, uint16_t, InputType, uint32_t,
    common::offset_t, const CompressionMetadata&, AdditionalArgs...)>;

using write_values_from_vector_func_t = write_values_to_page_func_t<common::ValueVector*>;

using write_values_func_t = write_values_to_page_func_t<const uint8_t*, const common::NullMask*>;

using filter_func_t = std::function<bool(common::offset_t, common::offset_t)>;

struct ColumnReadWriterFactory {
    static std::unique_ptr<ColumnReadWriter> createColumnReadWriter(common::PhysicalTypeID dataType,
        FileHandle* dataFH, ShadowFile* shadowFile);
};

class ColumnReadWriter {
public:
    ColumnReadWriter(FileHandle* dataFH, ShadowFile* shadowFile);

    virtual ~ColumnReadWriter() = default;

    virtual void readCompressedValueToPage(const transaction::Transaction* transaction,
        const ChunkState& state, common::offset_t nodeOffset, uint8_t* result,
        uint32_t offsetInResult, const read_value_from_page_func_t<uint8_t*>& readFunc) = 0;

    virtual void readCompressedValueToVector(const transaction::Transaction* transaction,
        const ChunkState& state, common::offset_t nodeOffset, common::ValueVector* result,
        uint32_t offsetInResult,
        const read_value_from_page_func_t<common::ValueVector*>& readFunc) = 0;

    virtual uint64_t readCompressedValuesToPage(const transaction::Transaction* transaction,
        const ChunkState& state, uint8_t* result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        const read_values_from_page_func_t<uint8_t*>& readFunc,
        const std::optional<filter_func_t>& filterFunc = {}) = 0;

    virtual uint64_t readCompressedValuesToVector(const transaction::Transaction* transaction,
        const ChunkState& state, common::ValueVector* result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        const read_values_from_page_func_t<common::ValueVector*>& readFunc,
        const std::optional<filter_func_t>& filterFunc = {}) = 0;

    virtual void writeValueToPageFromVector(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom,
        const write_values_from_vector_func_t& writeFromVectorFunc) = 0;

    virtual void writeValuesToPageFromBuffer(ChunkState& state, common::offset_t dstOffset,
        const uint8_t* data, const common::NullMask* nullChunkData, common::offset_t srcOffset,
        common::offset_t numValues, const write_values_func_t& writeFunc) = 0;

    void readFromPage(const transaction::Transaction* transaction, common::page_idx_t pageIdx,
        const std::function<void(uint8_t*)>& readFunc) const;

    void updatePageWithCursor(PageCursor cursor,
        const std::function<void(uint8_t*, common::offset_t)>& writeOp) const;

    static PageCursor getPageCursorForOffsetInGroup(common::offset_t offsetInChunk,
        common::page_idx_t groupPageIdx, uint64_t numValuesPerPage);

protected:
    static std::pair<common::offset_t, PageCursor> getOffsetAndCursor(common::offset_t nodeOffset,
        const ChunkState& state);

private:
    FileHandle* dataFH;
    ShadowFile* shadowFile;
};

} // namespace storage
} // namespace kuzu
