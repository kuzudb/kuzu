#pragma once

#include "base_csv_reader.h"
#include "common/types/types.h"
#include "function/scalar_function.h"
#include "function/table_functions.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"
#include "function/table_functions/scan_functions.h"

namespace kuzu {
namespace processor {

//! ParallelCSVReader is a class that reads values from a stream in parallel.
class ParallelCSVReader final : public BaseCSVReader {
    friend class ParallelParsingDriver;

public:
    ParallelCSVReader(
        const std::string& filePath, const common::ReaderConfig& readerConfig, uint64_t numColumns);

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

struct ParallelCSVScanSharedState final : public function::ScanSharedState {
    explicit ParallelCSVScanSharedState(
        const common::ReaderConfig readerConfig, uint64_t numRows, uint64_t numColumns)
        : ScanSharedState{std::move(readerConfig), numRows}, numColumns{numColumns} {}

    void setFileComplete(uint64_t completedFileIdx);

    uint64_t numColumns;
};

struct ParallelCSVScan {
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        function::TableFuncBindInput* input, catalog::CatalogContent* /*catalog*/);

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);

    static std::unique_ptr<function::TableFuncLocalState> initLocalState(
        function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* state);
};

} // namespace processor
} // namespace kuzu
