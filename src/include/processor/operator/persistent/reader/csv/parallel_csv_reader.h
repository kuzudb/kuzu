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
    ParallelCSVReader(const std::string& filePath, common::CSVOption option,
        CSVColumnInfo columnInfo, main::ClientContext* context);

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
    common::idx_t fileIdx;
};

struct ParallelCSVScanSharedState final : public function::ScanFileSharedState {
    common::CSVOption csvOption;
    CSVColumnInfo columnInfo;
    uint64_t numBlocksReadByFiles = 0;

    ParallelCSVScanSharedState(common::ReaderConfig readerConfig, uint64_t numRows,
        main::ClientContext* context, common::CSVOption csvOption, CSVColumnInfo columnInfo)
        : ScanFileSharedState{std::move(readerConfig), numRows, context},
          csvOption{std::move(csvOption)}, columnInfo{std::move(columnInfo)},
          numBlocksReadByFiles{0} {}

    void setFileComplete(uint64_t completedFileIdx);
};

struct ParallelCSVScan {
    static constexpr const char* name = "READ_CSV_PARALLEL";

    static function::function_set getFunctionSet();
};

} // namespace processor
} // namespace kuzu
