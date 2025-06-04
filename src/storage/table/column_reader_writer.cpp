#include "storage/table/column_reader_writer.h"

#include "alp/encode.hpp"
#include "common/utils.h"
#include "common/vector/value_vector.h"
#include "storage/compression/float_compression.h"
#include "storage/file_handle.h"
#include "storage/shadow_utils.h"
#include "storage/storage_utils.h"
#include "storage/table/column_chunk_data.h"
#include "storage/table/column_chunk_metadata.h"
#include "transaction/transaction.h"
#include <concepts>

namespace kuzu::storage {

using namespace common;
using namespace transaction;

namespace {
[[maybe_unused]] bool isPageIdxValid(page_idx_t pageIdx, const ColumnChunkMetadata& metadata) {
    return (metadata.getStartPageIdx() <= pageIdx &&
               pageIdx < metadata.getStartPageIdx() + metadata.getNumPages()) ||
           (pageIdx == INVALID_PAGE_IDX && metadata.compMeta.isConstant());
}

template<typename T, typename InputType, typename ElementType>
concept WriteToPageHelper = requires(T obj, InputType input, offset_t offset, ElementType element) {
    { obj.getValue(offset) } -> std::same_as<ElementType>;
    { obj.setValue(offset, element) };
    { obj.getData() } -> std::same_as<InputType>;
} && std::is_constructible_v<T, InputType, size_t>;

template<std::floating_point T>
struct WriteToBufferHelper {
    WriteToBufferHelper(const uint8_t* inputBuffer, size_t numValues)
        : inputBuffer(inputBuffer),
          outputBuffer(std::make_unique<uint8_t[]>(numValues * sizeof(T))) {}
    T getValue(offset_t offset) const { return reinterpret_cast<const T*>(inputBuffer)[offset]; }
    void setValue(offset_t offset, T element) {
        reinterpret_cast<T*>(outputBuffer.get())[offset] = element;
    }
    const uint8_t* getData() const { return outputBuffer.get(); }

    const uint8_t* inputBuffer;
    std::unique_ptr<uint8_t[]> outputBuffer;
};
static_assert(WriteToPageHelper<WriteToBufferHelper<double>, const uint8_t*, double>);
static_assert(WriteToPageHelper<WriteToBufferHelper<float>, const uint8_t*, float>);

template<std::floating_point T>
struct WriteToVectorHelper {
    explicit WriteToVectorHelper(ValueVector* inputVec, size_t /*numValues*/)
        : inputVec(inputVec),
          outputVec(std::is_same_v<double, T> ? LogicalTypeID::DOUBLE : LogicalTypeID::FLOAT) {}
    T getValue(offset_t offset) const { return inputVec->getValue<T>(offset); }
    void setValue(offset_t offset, T element) { outputVec.setValue(offset, element); }
    ValueVector* getData() { return &outputVec; }

    ValueVector* inputVec;
    ValueVector outputVec;
};
static_assert(WriteToPageHelper<WriteToVectorHelper<double>, ValueVector*, double>);
static_assert(WriteToPageHelper<WriteToVectorHelper<float>, ValueVector*, float>);

template<typename InputType, std::floating_point T>
decltype(auto) getWriteToPageBufferHelper(InputType input, size_t numValues) {
    if constexpr (std::is_same_v<InputType, const uint8_t*>) {
        return WriteToBufferHelper<T>(input, numValues);
    } else {
        return WriteToVectorHelper<T>(input, numValues);
    }
}

template<std::floating_point T>
class FloatColumnReadWriter;

class DefaultColumnReadWriter final : public ColumnReadWriter {
public:
    DefaultColumnReadWriter(FileHandle* dataFH, ShadowFile* shadowFile)
        : ColumnReadWriter(dataFH, shadowFile) {}

    void readCompressedValueToPage(const Transaction* transaction, const ChunkState& state,
        offset_t nodeOffset, uint8_t* result, uint32_t offsetInResult,
        const read_value_from_page_func_t<uint8_t*>& readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, state);
        readCompressedValue<uint8_t*>(transaction, state.metadata, cursor, offsetInChunk, result,
            offsetInResult, readFunc);
    }

    void readCompressedValueToVector(const Transaction* transaction, const ChunkState& state,
        offset_t nodeOffset, ValueVector* result, uint32_t offsetInResult,
        const read_value_from_page_func_t<ValueVector*>& readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, state);
        readCompressedValue<ValueVector*>(transaction, state.metadata, cursor, offsetInChunk,
            result, offsetInResult, readFunc);
    }

    uint64_t readCompressedValuesToPage(const Transaction* transaction, const ChunkState& state,
        uint8_t* result, uint32_t startOffsetInResult, uint64_t startNodeOffset,
        uint64_t endNodeOffset, const read_values_from_page_func_t<uint8_t*>& readFunc,
        const std::optional<filter_func_t>& filterFunc) override {
        return readCompressedValues(transaction, state, result, startOffsetInResult,
            startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    uint64_t readCompressedValuesToVector(const Transaction* transaction, const ChunkState& state,
        ValueVector* result, uint32_t startOffsetInResult, uint64_t startNodeOffset,
        uint64_t endNodeOffset, const read_values_from_page_func_t<ValueVector*>& readFunc,
        const std::optional<filter_func_t>& filterFunc) override {
        return readCompressedValues(transaction, state, result, startOffsetInResult,
            startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    void writeValueToPageFromVector(ChunkState& state, offset_t offsetInChunk,
        ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom,
        const write_values_from_vector_func_t& writeFromVectorFunc) override {
        writeValuesToPage(state, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom, 1,
            writeFromVectorFunc, &vectorToWriteFrom->getNullMask());
    }

    void writeValuesToPageFromBuffer(ChunkState& state, offset_t dstOffset, const uint8_t* data,
        const NullMask* nullChunkData, offset_t srcOffset, offset_t numValues,
        const write_values_func_t& writeFunc) override {
        writeValuesToPage(state, dstOffset, data, srcOffset, numValues, writeFunc, nullChunkData);
    }

    template<typename InputType, typename... AdditionalArgs>
    void writeValuesToPage(ChunkState& state, offset_t dstOffset, InputType data,
        offset_t srcOffset, offset_t numValues,
        const write_values_to_page_func_t<InputType, AdditionalArgs...>& writeFunc,
        const NullMask* nullMask) {
        auto numValuesWritten = 0u;
        auto cursor = getPageCursorForOffsetInGroup(dstOffset, state.metadata.getStartPageIdx(),
            state.numValuesPerPage);
        while (numValuesWritten < numValues) {
            KU_ASSERT(
                cursor.pageIdx == INVALID_PAGE_IDX /*constant compression*/ ||
                cursor.pageIdx < state.metadata.getStartPageIdx() + state.metadata.getNumPages());
            auto numValuesToWriteInPage = std::min(numValues - numValuesWritten,
                state.numValuesPerPage - cursor.elemPosInPage);
            updatePageWithCursor(cursor, [&](auto frame, auto offsetInPage) {
                if constexpr (std::is_same_v<InputType, ValueVector*>) {
                    writeFunc(frame, offsetInPage, data, srcOffset + numValuesWritten,
                        numValuesToWriteInPage, state.metadata.compMeta);
                } else {
                    writeFunc(frame, offsetInPage, data, srcOffset + numValuesWritten,
                        numValuesToWriteInPage, state.metadata.compMeta, nullMask);
                }
            });
            numValuesWritten += numValuesToWriteInPage;
            cursor.nextPage();
        }
    }

    template<typename OutputType>
    void readCompressedValue(const Transaction* transaction, const ColumnChunkMetadata& metadata,
        PageCursor cursor, offset_t /*offsetInChunk*/, OutputType result, uint32_t offsetInResult,
        const read_value_from_page_func_t<OutputType>& readFunc) {

        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            readFunc(frame, cursor, result, offsetInResult, 1 /* numValuesToRead */,
                metadata.compMeta);
        });
    }

    template<typename OutputType>
    uint64_t readCompressedValues(const Transaction* transaction, const ChunkState& state,
        OutputType result, uint32_t startOffsetInResult, uint64_t startNodeOffset,
        uint64_t endNodeOffset, const read_values_from_page_func_t<OutputType>& readFunc,
        const std::optional<filter_func_t>& filterFunc) {
        const ColumnChunkMetadata& chunkMeta = state.metadata;
        const auto numValuesToScan = endNodeOffset - startNodeOffset;
        if (numValuesToScan == 0) {
            return 0;
        }

        auto pageCursor = getPageCursorForOffsetInGroup(startNodeOffset,
            chunkMeta.getStartPageIdx(), state.numValuesPerPage);
        KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, chunkMeta));

        uint64_t numValuesScanned = 0;
        while (numValuesScanned < numValuesToScan) {
            uint64_t numValuesToScanInPage =
                std::min(state.numValuesPerPage - pageCursor.elemPosInPage,
                    numValuesToScan - numValuesScanned);
            KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, chunkMeta));
            if (!filterFunc.has_value() ||
                filterFunc.value()(numValuesScanned, numValuesScanned + numValuesToScanInPage)) {

                const auto readFromPageFunc = [&](uint8_t* frame) -> void {
                    readFunc(frame, pageCursor, result, numValuesScanned + startOffsetInResult,
                        numValuesToScanInPage, chunkMeta.compMeta);
                };
                readFromPage(transaction, pageCursor.pageIdx, std::cref(readFromPageFunc));
            }
            numValuesScanned += numValuesToScanInPage;
            pageCursor.nextPage();
        }

        return numValuesScanned;
    }
};

template<std::floating_point T>
class FloatColumnReadWriter final : public ColumnReadWriter {
public:
    FloatColumnReadWriter(FileHandle* dataFH, ShadowFile* shadowFile)
        : ColumnReadWriter(dataFH, shadowFile),
          defaultReader(std::make_unique<DefaultColumnReadWriter>(dataFH, shadowFile)) {}

    void readCompressedValueToPage(const Transaction* transaction, const ChunkState& state,
        offset_t nodeOffset, uint8_t* result, uint32_t offsetInResult,
        const read_value_from_page_func_t<uint8_t*>& readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, state);
        readCompressedValue<uint8_t*>(transaction, state, offsetInChunk, result, offsetInResult,
            readFunc);
    }

    void readCompressedValueToVector(const Transaction* transaction, const ChunkState& state,
        offset_t nodeOffset, ValueVector* result, uint32_t offsetInResult,
        const read_value_from_page_func_t<ValueVector*>& readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, state);
        readCompressedValue<ValueVector*>(transaction, state, offsetInChunk, result, offsetInResult,
            readFunc);
    }

    uint64_t readCompressedValuesToPage(const Transaction* transaction, const ChunkState& state,
        uint8_t* result, uint32_t startOffsetInResult, uint64_t startNodeOffset,
        uint64_t endNodeOffset, const read_values_from_page_func_t<uint8_t*>& readFunc,
        const std::optional<filter_func_t>& filterFunc) override {
        return readCompressedValues(transaction, state, result, startOffsetInResult,
            startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    uint64_t readCompressedValuesToVector(const Transaction* transaction, const ChunkState& state,
        ValueVector* result, uint32_t startOffsetInResult, uint64_t startNodeOffset,
        uint64_t endNodeOffset, const read_values_from_page_func_t<ValueVector*>& readFunc,
        const std::optional<filter_func_t>& filterFunc) override {
        return readCompressedValues(transaction, state, result, startOffsetInResult,
            startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    void writeValueToPageFromVector(ChunkState& state, offset_t offsetInChunk,
        ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom,
        const write_values_from_vector_func_t& writeFromVectorFunc) override {
        if (state.metadata.compMeta.compression != CompressionType::ALP) {
            return defaultReader->writeValueToPageFromVector(state, offsetInChunk,
                vectorToWriteFrom, posInVectorToWriteFrom, writeFromVectorFunc);
        }

        writeValuesToPage(state, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom, 1,
            writeFromVectorFunc, &vectorToWriteFrom->getNullMask());
    }

    void writeValuesToPageFromBuffer(ChunkState& state, offset_t dstOffset, const uint8_t* data,
        const NullMask* nullChunkData, offset_t srcOffset, offset_t numValues,
        const write_values_func_t& writeFunc) override {
        if (state.metadata.compMeta.compression != CompressionType::ALP) {
            defaultReader->writeValuesToPageFromBuffer(state, dstOffset, data, nullChunkData,
                srcOffset, numValues, writeFunc);
            return;
        }

        writeValuesToPage(state, dstOffset, data, srcOffset, numValues, writeFunc, nullChunkData);
    }

private:
    template<typename OutputType>
    void patchFloatExceptions(const ChunkState& state, offset_t startOffsetInChunk,
        size_t numValuesToScan, OutputType result, offset_t startOffsetInResult,
        const std::optional<filter_func_t>& filterFunc) {
        auto* exceptionChunk = state.getExceptionChunkConst<T>();
        offset_t curExceptionIdx =
            exceptionChunk->findFirstExceptionAtOrPastOffset(startOffsetInChunk);
        for (; curExceptionIdx < exceptionChunk->getExceptionCount(); ++curExceptionIdx) {
            const auto curException = exceptionChunk->getExceptionAt(curExceptionIdx);
            KU_ASSERT(curExceptionIdx == 0 ||
                      curException.posInChunk >
                          exceptionChunk->getExceptionAt(curExceptionIdx - 1).posInChunk);
            KU_ASSERT(curException.posInChunk >= curExceptionIdx);
            if (curException.posInChunk >= startOffsetInChunk + numValuesToScan) {
                break;
            }
            const offset_t offsetInResult =
                startOffsetInResult + curException.posInChunk - startOffsetInChunk;
            if (!filterFunc.has_value() || filterFunc.value()(offsetInResult, offsetInResult + 1)) {
                if constexpr (std::is_same_v<uint8_t*, OutputType>) {
                    reinterpret_cast<T*>(result)[offsetInResult] = curException.value;
                } else {
                    static_assert(std::is_same_v<ValueVector*, std::remove_cvref_t<OutputType>>);
                    reinterpret_cast<T*>(result->getData())[offsetInResult] = curException.value;
                }
            }
        }
    }

    template<typename OutputType>
    void readCompressedValue(const Transaction* transaction, const ChunkState& state,
        offset_t offsetInChunk, OutputType result, uint32_t offsetInResult,
        const read_value_from_page_func_t<OutputType>& readFunc) {
        RUNTIME_CHECK(const ColumnChunkMetadata& metadata = state.metadata);
        KU_ASSERT(metadata.compMeta.compression == CompressionType::ALP ||
                  metadata.compMeta.compression == CompressionType::CONSTANT ||
                  metadata.compMeta.compression == CompressionType::UNCOMPRESSED);
        std::optional<filter_func_t> filterFunc{};
        readCompressedValues(transaction, state, result, offsetInResult, offsetInChunk,
            offsetInChunk + 1, readFunc, filterFunc);
    }

    template<typename OutputType>
    uint64_t readCompressedValues(const Transaction* transaction, const ChunkState& state,
        OutputType result, uint32_t startOffsetInResult, uint64_t startNodeOffset,
        uint64_t endNodeOffset, const read_values_from_page_func_t<OutputType>& readFunc,
        const std::optional<filter_func_t>& filterFunc) {
        const ColumnChunkMetadata& metadata = state.metadata;
        KU_ASSERT(metadata.compMeta.compression == CompressionType::ALP ||
                  metadata.compMeta.compression == CompressionType::CONSTANT ||
                  metadata.compMeta.compression == CompressionType::UNCOMPRESSED);

        const uint64_t numValuesToScan = endNodeOffset - startNodeOffset;
        const uint64_t numValuesScanned =
            defaultReader->readCompressedValues(transaction, state, result, startOffsetInResult,
                startNodeOffset, endNodeOffset, readFunc, std::optional<filter_func_t>{filterFunc});

        if (metadata.compMeta.compression == CompressionType::ALP && numValuesScanned > 0) {
            // we pass in copies of the filter func as it can hold state which may need resetting
            // between scanning passes
            patchFloatExceptions(state, startNodeOffset, numValuesToScan, result,
                startOffsetInResult, std::optional<filter_func_t>{filterFunc});
        }

        return numValuesScanned;
    }

    template<typename InputType, typename... AdditionalArgs>
    void writeValuesToPage(ChunkState& state, offset_t offsetInChunk, InputType data,
        uint32_t srcOffset, size_t numValues,
        const write_values_to_page_func_t<InputType, AdditionalArgs...>& writeFunc,
        const NullMask* nullMask) {
        const ColumnChunkMetadata& metadata = state.metadata;

        auto* exceptionChunk = state.getExceptionChunk<T>();

        auto writeToPageBufferHelper = getWriteToPageBufferHelper<InputType, T>(data, numValues);

        const auto bitpackHeader = FloatCompression<T>::getBitpackInfo(state.metadata.compMeta);
        offset_t curExceptionIdx = exceptionChunk->findFirstExceptionAtOrPastOffset(offsetInChunk);

        const auto maxWrittenPosInChunk = offsetInChunk + numValues;
        uint32_t curExceptionPosInChunk =
            (curExceptionIdx < exceptionChunk->getExceptionCount()) ?
                exceptionChunk->getExceptionAt(curExceptionIdx).posInChunk :
                maxWrittenPosInChunk;

        for (size_t i = 0; i < numValues; ++i) {
            const size_t writeOffset = offsetInChunk + i;
            const size_t readOffset = srcOffset + i;

            if (nullMask && nullMask->isNull(readOffset)) {
                continue;
            }

            while (curExceptionPosInChunk < writeOffset) {
                ++curExceptionIdx;
                if (curExceptionIdx < exceptionChunk->getExceptionCount()) {
                    curExceptionPosInChunk =
                        exceptionChunk->getExceptionAt(curExceptionIdx).posInChunk;
                } else {
                    curExceptionPosInChunk = maxWrittenPosInChunk;
                }
            }

            const T newValue = writeToPageBufferHelper.getValue(readOffset);
            const auto* floatMetadata = metadata.compMeta.floatMetadata();
            const auto encodedValue =
                alp::AlpEncode<T>::encode_value(newValue, floatMetadata->fac, floatMetadata->exp);
            const T decodedValue = alp::AlpDecode<T>::decode_value(encodedValue, floatMetadata->fac,
                floatMetadata->exp);

            bool newValueIsException = newValue != decodedValue;
            writeToPageBufferHelper.setValue(i,
                newValueIsException ? bitpackHeader.offset : newValue);

            // if the previous value was an exception
            // either overwrite it (if the new value is also an exception) or remove it
            if (curExceptionPosInChunk == writeOffset) {
                if (newValueIsException) {
                    exceptionChunk->writeException(
                        EncodeException<T>{newValue, safeIntegerConversion<uint32_t>(writeOffset)},
                        curExceptionIdx);
                } else {
                    exceptionChunk->removeExceptionAt(curExceptionIdx);
                }
            } else if (newValueIsException) {
                exceptionChunk->addException(
                    EncodeException<T>{newValue, safeIntegerConversion<uint32_t>(writeOffset)});
            }
        }

        defaultReader->writeValuesToPage(state, offsetInChunk, writeToPageBufferHelper.getData(), 0,
            numValues, writeFunc, nullMask);
    }

    std::unique_ptr<DefaultColumnReadWriter> defaultReader;
};

} // namespace

std::unique_ptr<ColumnReadWriter> ColumnReadWriterFactory::createColumnReadWriter(
    PhysicalTypeID dataType, FileHandle* dataFH, ShadowFile* shadowFile) {
    switch (dataType) {
    case PhysicalTypeID::FLOAT:
        return std::make_unique<FloatColumnReadWriter<float>>(dataFH, shadowFile);
    case PhysicalTypeID::DOUBLE:
        return std::make_unique<FloatColumnReadWriter<double>>(dataFH, shadowFile);
    default:
        return std::make_unique<DefaultColumnReadWriter>(dataFH, shadowFile);
    }
}

ColumnReadWriter::ColumnReadWriter(FileHandle* dataFH, ShadowFile* shadowFile)
    : dataFH(dataFH), shadowFile(shadowFile) {}

void ColumnReadWriter::readFromPage(const Transaction* transaction, page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& readFunc) const {
    // For constant compression, call read on a nullptr since there is no data on disk and
    // decompression only requires metadata
    if (pageIdx == INVALID_PAGE_IDX) {
        return readFunc(nullptr);
    }
    auto [fileHandleToPin, pageIdxToPin] = ShadowUtils::getFileHandleAndPhysicalPageIdxToPin(
        *dataFH, pageIdx, *shadowFile, transaction->getType());
    fileHandleToPin->optimisticReadPage(pageIdxToPin, readFunc);
}

void ColumnReadWriter::updatePageWithCursor(PageCursor cursor,
    const std::function<void(uint8_t*, offset_t)>& writeOp) const {
    if (cursor.pageIdx == INVALID_PAGE_IDX) {
        writeOp(nullptr, cursor.elemPosInPage);
        return;
    }
    KU_ASSERT(cursor.pageIdx < dataFH->getNumPages());

    ShadowUtils::updatePage(*dataFH, cursor.pageIdx, false /*insertingNewPage*/, *shadowFile,
        [&](auto frame) { writeOp(frame, cursor.elemPosInPage); });
}

PageCursor ColumnReadWriter::getPageCursorForOffsetInGroup(offset_t offsetInChunk,
    page_idx_t groupPageIdx, uint64_t numValuesPerPage) {
    auto pageCursor = PageUtils::getPageCursorForPos(offsetInChunk, numValuesPerPage);
    pageCursor.pageIdx += groupPageIdx;
    return pageCursor;
}

std::pair<offset_t, PageCursor> ColumnReadWriter::getOffsetAndCursor(offset_t nodeOffset,
    const ChunkState& state) {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    auto cursor = getPageCursorForOffsetInGroup(offsetInChunk, state.metadata.getStartPageIdx(),
        state.numValuesPerPage);
    KU_ASSERT(isPageIdxValid(cursor.pageIdx, state.metadata));

    return {offsetInChunk, cursor};
}

} // namespace kuzu::storage
