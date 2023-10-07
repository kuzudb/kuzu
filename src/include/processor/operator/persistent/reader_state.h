#pragma once

#include "common/data_chunk/data_chunk.h"
#include "processor/operator/persistent/reader_functions.h"

namespace kuzu {
namespace processor {

struct ReaderMorsel {
    common::vector_idx_t fileIdx;
    common::block_idx_t blockIdx;

    ReaderMorsel() : fileIdx{common::INVALID_VECTOR_IDX}, blockIdx{common::INVALID_BLOCK_IDX} {}
    ReaderMorsel(common::vector_idx_t fileIdx, common::block_idx_t blockIdx)
        : fileIdx{fileIdx}, blockIdx{blockIdx} {}
};

class LeftArrowArrays {
public:
    explicit LeftArrowArrays() : leftNumRows{0} {}

    inline uint64_t getLeftNumRows() const { return leftNumRows; }

    void appendFromDataChunk(common::DataChunk* dataChunk);

    void appendToDataChunk(common::DataChunk* dataChunk, uint64_t numRowsToAppend);

private:
    common::row_idx_t leftNumRows;
    std::vector<arrow::ArrayVector> leftArrays;
};

class ReaderSharedState {
    friend class Reader;

public:
    enum class ReadMode : uint8_t {
        PARALLEL = 0,
        SERIAL = 1,
    };

    explicit ReaderSharedState(std::unique_ptr<common::ReaderConfig> readerConfig)
        : readerConfig{std::move(readerConfig)}, numRows{common::INVALID_ROW_IDX}, currFileIdx{0},
          currBlockIdx{0}, currRowIdx{0} {}

    void initialize(storage::MemoryManager* memoryManager, common::TableType tableType);
    void validate() const;
    void countRows(storage::MemoryManager* memoryManager);

    // Signal that we are done the given file.
    // No-op if we have already moved to the next file.
    template<ReadMode READ_MODE>
    void doneFile(common::vector_idx_t fileIdx);

    template<ReadMode READ_MODE>
    std::unique_ptr<ReaderMorsel> getMorsel();

    inline common::row_idx_t& getNumRowsRef() { return std::ref(numRows); }

public:
    validate_func_t validateFunc;
    init_reader_data_func_t initFunc;
    count_rows_func_t countRowsFunc;
    std::shared_ptr<ReaderFunctionData> readFuncData;

    common::row_idx_t numRows;

    common::vector_idx_t currFileIdx;
    common::block_idx_t currBlockIdx;
    std::atomic<common::row_idx_t> currRowIdx;

private:
    std::mutex mtx;
    std::unique_ptr<common::ReaderConfig> readerConfig;
    LeftArrowArrays leftArrowArrays;
};

} // namespace processor
} // namespace kuzu
