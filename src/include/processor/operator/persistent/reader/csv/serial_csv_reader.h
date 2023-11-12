#pragma once

#include "base_csv_reader.h"
#include "function/scalar_function.h"
#include "function/table_functions/bind_data.h"
#include "function/table_functions/bind_input.h"
#include "function/table_functions/scan_functions.h"

namespace kuzu {
namespace processor {

//! Serial CSV reader is a class that reads values from a stream in a single thread.
class SerialCSVReader final : public BaseCSVReader {
public:
    SerialCSVReader(
        const std::string& filePath, const common::ReaderConfig& readerConfig, uint64_t numColumns);

    //! Sniffs CSV dialect and determines skip rows, header row, column types and column names
    std::vector<std::pair<std::string, common::LogicalType>> sniffCSV();
    uint64_t parseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk) override;

protected:
    void handleQuotedNewline() override {}
};

struct SerialCSVScanSharedState final : public function::ScanSharedState {
    explicit SerialCSVScanSharedState(
        const common::ReaderConfig readerConfig, uint64_t numRows, uint64_t numColumns)
        : ScanSharedState{std::move(readerConfig), numRows}, numColumns{numColumns} {
        initReader();
    }

    void read(common::DataChunk& outputChunk);

    void initReader();

    std::unique_ptr<SerialCSVReader> reader;
    uint64_t numColumns;
};

struct SerialCSVScan {
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        function::TableFuncBindInput* input, catalog::CatalogContent* /*catalog*/);

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);

    static std::unique_ptr<function::TableFuncLocalState> initLocalState(
        function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* /*state*/);

    static void bindColumns(const common::ReaderConfig& readerConfig,
        std::vector<std::string>& columnNames,
        std::vector<std::unique_ptr<common::LogicalType>>& columnTypes);
    static void bindColumns(const common::ReaderConfig& readerConfig, uint32_t fileIdx,
        std::vector<std::string>& columnNames,
        std::vector<std::unique_ptr<common::LogicalType>>& columnTypes);
};

} // namespace processor
} // namespace kuzu
