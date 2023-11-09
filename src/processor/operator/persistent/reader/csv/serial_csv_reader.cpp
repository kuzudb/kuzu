#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"

#include "common/string_format.h"
#include "processor/operator/persistent/reader/csv/driver.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

SerialCSVReader::SerialCSVReader(
    const std::string& filePath, const common::ReaderConfig& readerConfig)
    : BaseCSVReader{filePath, readerConfig} {}

std::vector<std::pair<std::string, LogicalType>> SerialCSVReader::sniffCSV() {
    readBOM();
    numColumnsDetected = 0;

    if (csvReaderConfig.hasHeader) {
        SniffCSVNameAndTypeDriver driver;
        parseCSV(driver);
        return driver.columns;
    } else {
        SniffCSVColumnCountDriver driver;
        parseCSV(driver);
        std::vector<std::pair<std::string, LogicalType>> columns;
        columns.reserve(driver.numColumns);
        for (uint64_t i = 0; i < driver.numColumns; i++) {
            columns.emplace_back(stringFormat("column{}", i), LogicalTypeID::STRING);
        }
        return columns;
    }
}

uint64_t SerialCSVReader::parseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk) {
    currentBlockIdx = blockIdx;
    if (blockIdx == 0) {
        handleFirstBlock();
    }
    SerialParsingDriver driver(resultChunk, this);
    return parseCSV(driver);
}

void SerialCSVScanSharedState::read(common::DataChunk& outputChunk) {
    std::lock_guard<std::mutex> mtx{lock};
    do {
        if (fileIdx > readerConfig.getNumFiles()) {
            return;
        }
        uint64_t numRows = reader->parseBlock(0 /* unused by serial csv reader */, outputChunk);
        // TODO(Ziyi): parseBlock should set the selectedSize of dataChunk.
        outputChunk.state->selVector->selectedSize = numRows;
        if (numRows > 0) {
            return;
        }
        fileIdx++;
        initReader();
    } while (true);
}

void SerialCSVScanSharedState::initReader() {
    if (fileIdx < readerConfig.getNumFiles()) {
        reader = std::make_unique<SerialCSVReader>(readerConfig.filePaths[fileIdx], readerConfig);
    }
}

function_set SerialCSVScan::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<TableFunction>(READ_CSV_SERIAL_FUNC_NAME, tableFunc, bindFunc,
            initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

void SerialCSVScan::tableFunc(TableFunctionInput& input, DataChunk& outputChunk) {
    auto serialCSVScanSharedState = reinterpret_cast<SerialCSVScanSharedState*>(input.sharedState);
    serialCSVScanSharedState->read(outputChunk);
}

std::unique_ptr<function::TableFuncBindData> SerialCSVScan::bindFunc(
    main::ClientContext* /*context*/, function::TableFuncBindInput* input,
    catalog::CatalogContent* /*catalog*/) {
    auto csvScanBindInput = reinterpret_cast<function::ScanTableFuncBindInput*>(input);
    return std::make_unique<function::ScanBindData>(
        common::LogicalType::copy(csvScanBindInput->config.columnTypes),
        csvScanBindInput->config.columnNames, csvScanBindInput->config, csvScanBindInput->mm);
}

std::unique_ptr<function::TableFuncSharedState> SerialCSVScan::initSharedState(
    function::TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<function::ScanBindData*>(input.bindData);
    common::row_idx_t numRows = 0;
    for (const auto& path : bindData->config.filePaths) {
        auto reader = make_unique<SerialCSVReader>(path, bindData->config);
        numRows += reader->countRows();
    }
    return std::make_unique<SerialCSVScanSharedState>(bindData->config, numRows);
}

std::unique_ptr<function::TableFuncLocalState> SerialCSVScan::initLocalState(
    function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* /*state*/) {
    return std::make_unique<function::TableFuncLocalState>();
}

} // namespace processor
} // namespace kuzu
