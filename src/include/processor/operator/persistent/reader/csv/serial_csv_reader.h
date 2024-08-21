#pragma once

#include "base_csv_reader.h"
#include "function/function.h"
#include "function/table/bind_input.h"
#include "function/table/scan_functions.h"

namespace kuzu {
namespace processor {

//! Serial CSV reader is a class that reads values from a stream in a single thread.
class SerialCSVReader final : public BaseCSVReader {
public:
    SerialCSVReader(const std::string& filePath, common::CSVOption option, CSVColumnInfo columnInfo,
        main::ClientContext* context, SharedCSVFileErrorHandler* errorHandler,
        const function::ScanTableFuncBindInput* bindInput = nullptr);

    //! Sniffs CSV dialect and determines skip rows, header row, column types and column names
    std::vector<std::pair<std::string, common::LogicalType>> sniffCSV();
    uint64_t parseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk) override;

protected:
    bool handleQuotedNewline() override { return true; }

private:
    const function::ScanTableFuncBindInput* bindInput;
};

struct SerialCSVScanSharedState final : public function::ScanFileSharedState {
    std::unique_ptr<SerialCSVReader> reader;
    common::CSVOption csvOption;
    CSVColumnInfo columnInfo;
    uint64_t totalReadSizeByFile;
    std::shared_ptr<warning_counter_t> warningCounter;
    std::vector<SharedCSVFileErrorHandler> errorHandlers;

    SerialCSVScanSharedState(common::ReaderConfig readerConfig, uint64_t numRows,
        main::ClientContext* context, common::CSVOption csvOption, CSVColumnInfo columnInfo);

    void read(common::DataChunk& outputChunk);

    void initReader(main::ClientContext* context);
};

struct SerialCSVScan {
    static constexpr const char* name = "READ_CSV_SERIAL";

    static function::function_set getFunctionSet();
    static void bindColumns(const function::ScanTableFuncBindInput* bindInput,
        std::vector<std::string>& columnNames, std::vector<common::LogicalType>& columnTypes);
};

} // namespace processor
} // namespace kuzu
