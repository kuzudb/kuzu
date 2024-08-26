#include "storage/store/column_reader_writer.h"

#include "alp/encode.hpp"
#include "common/utils.h"
#include "storage/compression/float_compression.h"
#include "storage/shadow_utils.h"
#include "storage/storage_utils.h"
#include "transaction/transaction.h"
#include <concepts>

namespace kuzu::storage {

using namespace common;
using namespace transaction;
namespace {
[[maybe_unused]] bool isPageIdxValid(page_idx_t pageIdx, const ColumnChunkMetadata& metadata) {
    return (metadata.pageIdx <= pageIdx && pageIdx < metadata.pageIdx + metadata.numPages) ||
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

class DefaultColumnReadWriter : public ColumnReadWriter {
public:
    DefaultColumnReadWriter(DBFileID dbFileID, FileHandle* dataFH, BufferManager* bufferManager,
        ShadowFile* shadowFile)
        : ColumnReadWriter(dbFileID, dataFH, bufferManager, shadowFile) {}

    void readCompressedValueToPage(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, uint8_t* result, uint32_t offsetInResult,
        const read_value_from_page_func_t<uint8_t*>& readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, state);
        readCompressedValue<uint8_t*>(transaction, state.metadata, cursor, offsetInChunk, result,
            offsetInResult, readFunc);
    }

    void readCompressedValueToVector(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* result, uint32_t offsetInResult,
        const read_value_from_page_func_t<common::ValueVector*>& readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, state);
        readCompressedValue<ValueVector*>(transaction, state.metadata, cursor, offsetInChunk,
            result, offsetInResult, readFunc);
    }

    uint64_t readCompressedValuesToPage(transaction::Transaction* transaction,
        const ChunkState& state, uint8_t* result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        const read_values_from_page_func_t<uint8_t*>& readFunc,
        std::optional<filter_func_t> filterFunc) override {
        return readCompressedValues(transaction, state, result, startOffsetInResult,
            startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    uint64_t readCompressedValuesToVector(transaction::Transaction* transaction,
        const ChunkState& state, common::ValueVector* result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        const read_values_from_page_func_t<common::ValueVector*>& readFunc,
        std::optional<filter_func_t> filterFunc) override {
        return readCompressedValues(transaction, state, result, startOffsetInResult,
            startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    void writeValueToPageFromVector(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom,
        const write_values_from_vector_func_t& writeFromVectorFunc) override {
        writeValuesToPage(state, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom, 1,
            writeFromVectorFunc, &vectorToWriteFrom->getNullMask());
    }

    void writeValuesToPageFromBuffer(ChunkState& state, common::offset_t dstOffset,
        const uint8_t* data, const common::NullMask* nullChunkData, common::offset_t srcOffset,
        common::offset_t numValues, const write_values_func_t& writeFunc) override {
        writeValuesToPage(state, dstOffset, data, srcOffset, numValues, writeFunc, nullChunkData);
    }

    template<typename InputType, typename... AdditionalArgs>
    void writeValuesToPage(ChunkState& state, common::offset_t dstOffset, InputType data,
        common::offset_t srcOffset, common::offset_t numValues,
        const write_values_to_page_func_t<InputType, AdditionalArgs...>& writeFunc,
        const NullMask* nullMask) {
        auto numValuesWritten = 0u;
        auto cursor = getPageCursorForOffsetInGroup(dstOffset, state.metadata.pageIdx,
            state.numValuesPerPage);
        while (numValuesWritten < numValues) {
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
    void readCompressedValue(transaction::Transaction* transaction,
        const ColumnChunkMetadata& metadata, PageCursor cursor, common::offset_t /*offsetInChunk*/,
        OutputType result, uint32_t offsetInResult,
        const read_value_from_page_func_t<OutputType>& readFunc) {

        readFromPage(transaction, cursor.pageIdx, [&](uint8_t* frame) -> void {
            readFunc(frame, cursor, result, offsetInResult, 1 /* numValuesToRead */,
                metadata.compMeta);
        });
    }

    template<typename OutputType>
    uint64_t readCompressedValues(Transaction* transaction, const ChunkState& state,
        OutputType result, uint32_t startOffsetInResult, uint64_t startNodeOffset,
        uint64_t endNodeOffset, const read_values_from_page_func_t<OutputType>& readFunc,
        const std::optional<filter_func_t>& filterFunc) {
        const ColumnChunkMetadata& chunkMeta = state.metadata;
        const auto numValuesToScan = endNodeOffset - startNodeOffset;
        if (numValuesToScan == 0) {
            return 0;
        }

        auto pageCursor = getPageCursorForOffsetInGroup(startNodeOffset, chunkMeta.pageIdx,
            state.numValuesPerPage);
        KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, chunkMeta));

        uint64_t numValuesScanned = 0;
        while (numValuesScanned < numValuesToScan) {
            uint64_t numValuesToScanInPage =
                std::min(state.numValuesPerPage - pageCursor.elemPosInPage,
                    numValuesToScan - numValuesScanned);
            KU_ASSERT(isPageIdxValid(pageCursor.pageIdx, chunkMeta));
            if (!filterFunc.has_value() ||
                filterFunc.value()(numValuesScanned, numValuesScanned + numValuesToScanInPage)) {
                readFromPage(transaction, pageCursor.pageIdx, [&](uint8_t* frame) -> void {
                    readFunc(frame, pageCursor, result, numValuesScanned + startOffsetInResult,
                        numValuesToScanInPage, chunkMeta.compMeta);
                });
            }
            numValuesScanned += numValuesToScanInPage;
            pageCursor.nextPage();
        }

        return numValuesScanned;
    }
};

template<std::floating_point T>
class FloatColumnReadWriter : public ColumnReadWriter {
public:
    FloatColumnReadWriter(DBFileID dbFileID, FileHandle* dataFH, BufferManager* bufferManager,
        ShadowFile* shadowFile)
        : ColumnReadWriter(dbFileID, dataFH, bufferManager, shadowFile),
          defaultReader(std::make_unique<DefaultColumnReadWriter>(dbFileID, dataFH, bufferManager,
              shadowFile)) {}

    void readCompressedValueToPage(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, uint8_t* result, uint32_t offsetInResult,
        const read_value_from_page_func_t<uint8_t*>& readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, state);
        readCompressedValue<uint8_t*>(transaction, state, offsetInChunk, result, offsetInResult,
            readFunc);
    }

    void readCompressedValueToVector(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t nodeOffset, common::ValueVector* result, uint32_t offsetInResult,
        const read_value_from_page_func_t<common::ValueVector*>& readFunc) override {
        auto [offsetInChunk, cursor] = getOffsetAndCursor(nodeOffset, state);
        readCompressedValue<ValueVector*>(transaction, state, offsetInChunk, result, offsetInResult,
            readFunc);
    }

    uint64_t readCompressedValuesToPage(transaction::Transaction* transaction,
        const ChunkState& state, uint8_t* result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        const read_values_from_page_func_t<uint8_t*>& readFunc,
        std::optional<filter_func_t> filterFunc) override {
        return readCompressedValues(transaction, state, result, startOffsetInResult,
            startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    uint64_t readCompressedValuesToVector(transaction::Transaction* transaction,
        const ChunkState& state, common::ValueVector* result, uint32_t startOffsetInResult,
        uint64_t startNodeOffset, uint64_t endNodeOffset,
        const read_values_from_page_func_t<common::ValueVector*>& readFunc,
        std::optional<filter_func_t> filterFunc) override {
        return readCompressedValues(transaction, state, result, startOffsetInResult,
            startNodeOffset, endNodeOffset, readFunc, filterFunc);
    }

    void writeValueToPageFromVector(ChunkState& state, common::offset_t offsetInChunk,
        common::ValueVector* vectorToWriteFrom, uint32_t posInVectorToWriteFrom,
        const write_values_from_vector_func_t& writeFromVectorFunc) override {
        if (state.metadata.compMeta.compression != CompressionType::ALP) {
            return defaultReader->writeValueToPageFromVector(state, offsetInChunk,
                vectorToWriteFrom, posInVectorToWriteFrom, writeFromVectorFunc);
        }

        writeValuesToPage(state, offsetInChunk, vectorToWriteFrom, posInVectorToWriteFrom, 1,
            writeFromVectorFunc, &vectorToWriteFrom->getNullMask());
    }

    void writeValuesToPageFromBuffer(ChunkState& state, common::offset_t dstOffset,
        const uint8_t* data, const common::NullMask* nullChunkData, common::offset_t srcOffset,
        common::offset_t numValues, const write_values_func_t& writeFunc) override {
        if (state.metadata.compMeta.compression != CompressionType::ALP) {
            return defaultReader->writeValuesToPageFromBuffer(state, dstOffset, data, nullChunkData,
                srcOffset, numValues, writeFunc);
        }

        writeValuesToPage(state, dstOffset, data, srcOffset, numValues, writeFunc, nullChunkData);
    }

private:
    template<typename OutputType>
    void patchFloatExceptions(const ChunkState& state, offset_t startOffsetInChunk,
        size_t numValuesToScan, OutputType result, offset_t startOffsetInResult,
        std::optional<filter_func_t> filterFunc) {
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
    void readCompressedValue(transaction::Transaction* transaction, const ChunkState& state,
        common::offset_t offsetInChunk, OutputType result, uint32_t offsetInResult,
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
    uint64_t readCompressedValues(Transaction* transaction, const ChunkState& state,
        OutputType result, uint32_t startOffsetInResult, uint64_t startNodeOffset,
        uint64_t endNodeOffset, const read_values_from_page_func_t<OutputType>& readFunc,
        const std::optional<filter_func_t>& filterFunc) {
        const ColumnChunkMetadata& metadata = state.metadata;
        KU_ASSERT(metadata.compMeta.compression == CompressionType::ALP ||
                  metadata.compMeta.compression == CompressionType::CONSTANT ||
                  metadata.compMeta.compression == CompressionType::UNCOMPRESSED);

        const uint64_t numValuesToScan = endNodeOffset - startNodeOffset;
        const uint64_t numValuesScanned = defaultReader->readCompressedValues(transaction, state,
            result, startOffsetInResult, startNodeOffset, endNodeOffset, readFunc, filterFunc);

        if (metadata.compMeta.compression == CompressionType::ALP && numValuesScanned > 0) {
            patchFloatExceptions(state, startNodeOffset, numValuesToScan, result,
                startOffsetInResult, filterFunc);
        }

        return numValuesScanned;
    }

    template<typename InputType, typename... AdditionalArgs>
    void writeValuesToPage(ChunkState& state, common::offset_t offsetInChunk, InputType data,
        uint32_t srcOffset, size_t numValues,
        const write_values_to_page_func_t<InputType, AdditionalArgs...>& writeFunc,
        const NullMask* nullMask) {
        const ColumnChunkMetadata& metadata = state.metadata;

        auto* exceptionChunk = state.getExceptionChunk<T>();

        auto writeToPageBufferHelper = getWriteToPageBufferHelper<InputType, T>(data, numValues);

        const auto bitpackHeader = FloatCompression<T>::getBitpackInfo(state.metadata.compMeta);
        offset_t curExceptionIdx = exceptionChunk->findFirstExceptionAtOrPastOffset(offsetInChunk);
        for (size_t i = 0; i < numValues; ++i) {
            const size_t writeOffset = offsetInChunk + i;
            const size_t readOffset = srcOffset + i;

            if (nullMask && nullMask->isNull(readOffset)) {
                continue;
            }

            const T newValue = writeToPageBufferHelper.getValue(readOffset);
            const auto* floatMetadata = metadata.compMeta.floatMetadata();
            const int64_t encodedValue =
                alp::AlpEncode<T>::encode_value(newValue, floatMetadata->fac, floatMetadata->exp);
            const T decodedValue = alp::AlpDecode<T>::decode_value(encodedValue, floatMetadata->fac,
                floatMetadata->exp);

            bool newValueIsException = newValue != decodedValue;
            writeToPageBufferHelper.setValue(i,
                newValueIsException ? bitpackHeader.offset : newValue);

            // if the previous value was an exception
            // either overwrite it (if the new value is also an exception) or remove it
            if (curExceptionIdx < exceptionChunk->getExceptionCount() &&
                exceptionChunk->getExceptionAt(curExceptionIdx).posInChunk == writeOffset) {
                if (newValueIsException) {
                    exceptionChunk->writeException(
                        EncodeException<T>{newValue, safeIntegerConversion<uint32_t>(writeOffset)},
                        curExceptionIdx);
                } else {
                    exceptionChunk->removeExceptionAt(curExceptionIdx);
                }
                ++curExceptionIdx;
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
    common::PhysicalTypeID dataType, DBFileID dbFileID, FileHandle* dataFH,
    BufferManager* bufferManager, ShadowFile* shadowFile) {
    switch (dataType) {
    case common::PhysicalTypeID::FLOAT:
        return std::make_unique<FloatColumnReadWriter<float>>(dbFileID, dataFH, bufferManager,
            shadowFile);
    case common::PhysicalTypeID::DOUBLE:
        return std::make_unique<FloatColumnReadWriter<double>>(dbFileID, dataFH, bufferManager,
            shadowFile);
    default:
        return std::make_unique<DefaultColumnReadWriter>(dbFileID, dataFH, bufferManager,
            shadowFile);
    }
}

ColumnReadWriter::ColumnReadWriter(DBFileID dbFileID, FileHandle* dataFH,
    BufferManager* bufferManager, ShadowFile* shadowFile)
    : dbFileID(dbFileID), dataFH(dataFH), bufferManager(bufferManager), shadowFile(shadowFile) {}

void ColumnReadWriter::readFromPage(Transaction* transaction, page_idx_t pageIdx,
    const std::function<void(uint8_t*)>& readFunc) {
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
    const std::function<void(uint8_t*, common::offset_t)>& writeOp) const {
    bool insertingNewPage = false;
    if (cursor.pageIdx == INVALID_PAGE_IDX) {
        return writeOp(nullptr, cursor.elemPosInPage);
    }
    if (cursor.pageIdx >= dataFH->getNumPages()) {
        KU_ASSERT(cursor.pageIdx == dataFH->getNumPages());
        ShadowUtils::insertNewPage(*dataFH, dbFileID, *shadowFile);
        insertingNewPage = true;
    }
    ShadowUtils::updatePage(*dataFH, dbFileID, cursor.pageIdx, insertingNewPage, *shadowFile,
        [&](auto frame) { writeOp(frame, cursor.elemPosInPage); });
}

PageCursor ColumnReadWriter::getPageCursorForOffsetInGroup(offset_t offsetInChunk,
    page_idx_t groupPageIdx, uint64_t numValuesPerPage) const {
    auto pageCursor = PageUtils::getPageCursorForPos(offsetInChunk, numValuesPerPage);
    pageCursor.pageIdx += groupPageIdx;
    return pageCursor;
}

std::pair<common::offset_t, PageCursor> ColumnReadWriter::getOffsetAndCursor(
    common::offset_t nodeOffset, const ChunkState& state) const {
    auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(nodeOffset);
    auto cursor = getPageCursorForOffsetInGroup(offsetInChunk, state.metadata.pageIdx,
        state.numValuesPerPage);
    KU_ASSERT(isPageIdxValid(cursor.pageIdx, state.metadata));

    return {offsetInChunk, cursor};
}

} // namespace kuzu::storage
