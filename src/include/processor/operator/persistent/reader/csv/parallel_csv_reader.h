#pragma once

#include "base_csv_reader.h"
#include "common/types/types.h"
#include "function/function.h"
#include "function/table/bind_input.h"
#include "function/table/scan_functions.h"
#include "function/table_functions.h"

namespace kuzu {
namespace processor {

//! ParallelCSVReader is a class that reads values from a stream in parallel.
class ParallelCSVReader final : public BaseCSVReader {
    friend class ParallelParsingDriver;

public:
    ParallelCSVReader(const std::string& filePath, common::CSVOption option, uint64_t numColumns,
        main::ClientContext* context);

    bool hasMoreToRead() const;
    uint64_t parseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk) override;
    uint64_t continueBlock(common::DataChunk& resultChunk);

protected:
    void handleQuotedNewline() override;

private:
    bool finishedBlock() const;
    void seekToBlockStart();
};

struct ParallelCSVLocalState final : public function::TableFuncLocalState {
    std::unique_ptr<ParallelCSVReader> reader;
    uint64_t fileIdx;
};

struct ParallelCSVScanSharedState final : public function::ScanFileSharedState {
    explicit ParallelCSVScanSharedState(common::ReaderConfig readerConfig, uint64_t numRows,
        uint64_t numColumns, main::ClientContext* context, common::CSVReaderConfig csvReaderConfig)
        : ScanFileSharedState{std::move(readerConfig), numRows, context}, numColumns{numColumns},
          numBlocksReadByFiles{0}, csvReaderConfig{std::move(csvReaderConfig)} {}

    void setFileComplete(uint64_t completedFileIdx);

    uint64_t numColumns;
    uint64_t numBlocksReadByFiles = 0;
    common::CSVReaderConfig csvReaderConfig;
};

struct ParallelCSVScan {
    static constexpr const char* name = "READ_CSV_PARALLEL";

    static function::function_set getFunctionSet();
};

} // namespace processor
} // namespace kuzu
