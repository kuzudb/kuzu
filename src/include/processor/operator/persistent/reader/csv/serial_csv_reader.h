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
    uint64_t totalReadSizeByFile;
    common::CSVReaderConfig csvReaderConfig;

    SerialCSVScanSharedState(common::ReaderConfig readerConfig, uint64_t numRows,
        uint64_t numColumns, common::CSVReaderConfig csvReaderConfig, main::ClientContext* context)
        : ScanFileSharedState{std::move(readerConfig), numRows, context}, numColumns{numColumns},
          totalReadSizeByFile{0}, csvReaderConfig{std::move(csvReaderConfig)} {
        initReader(context);
    }

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
