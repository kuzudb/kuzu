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
    SerialCSVReader(const std::string& filePath, common::CSVOption option, uint64_t numColumns,
        main::ClientContext* context);

    //! Sniffs CSV dialect and determines skip rows, header row, column types and column names
    std::vector<std::pair<std::string, common::LogicalType>> sniffCSV();
    uint64_t parseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk) override;

protected:
    void handleQuotedNewline() override {}
};

struct SerialCSVScanSharedState final : public function::ScanFileSharedState {
    std::unique_ptr<SerialCSVReader> reader;
    uint64_t numColumns;
    common::CSVReaderConfig csvReaderConfig;

    SerialCSVScanSharedState(common::ReaderConfig readerConfig, uint64_t numRows,
        uint64_t numColumns, common::CSVReaderConfig csvReaderConfig, main::ClientContext* context)
        : ScanFileSharedState{std::move(readerConfig), numRows, context}, numColumns{numColumns},
          csvReaderConfig{std::move(csvReaderConfig)} {
        initReader(context);
    }

    void read(common::DataChunk& outputChunk);

    void initReader(main::ClientContext* context);
};

struct SerialCSVScan {
    static function::function_set getFunctionSet();

    static void tableFunc(function::TableFunctionInput& input, common::DataChunk& outputChunk);

    static std::unique_ptr<function::TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
        function::TableFuncBindInput* input, catalog::Catalog* /*catalog*/,
        storage::StorageManager* /*storageManager*/);

    static std::unique_ptr<function::TableFuncSharedState> initSharedState(
        function::TableFunctionInitInput& input);

    static std::unique_ptr<function::TableFuncLocalState> initLocalState(
        function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* /*state*/,
        storage::MemoryManager* /*mm*/);

    static void bindColumns(const function::ScanTableFuncBindInput* bindInput,
        std::vector<std::string>& columnNames, std::vector<common::LogicalType>& columnTypes);
    static void bindColumns(const function::ScanTableFuncBindInput* bindInput, uint32_t fileIdx,
        std::vector<std::string>& columnNames, std::vector<common::LogicalType>& columnTypes);
};

} // namespace processor
} // namespace kuzu
