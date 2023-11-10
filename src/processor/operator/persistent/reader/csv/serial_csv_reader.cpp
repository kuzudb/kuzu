#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"

#include "common/string_format.h"
#include "processor/operator/persistent/reader/csv/driver.h"
#include "processor/operator/persistent/reader/reader_bind_utils.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

SerialCSVReader::SerialCSVReader(
    const std::string& filePath, const common::ReaderConfig& readerConfig, uint64_t numColumns)
    : BaseCSVReader{filePath, readerConfig, numColumns} {}

std::vector<std::pair<std::string, LogicalType>> SerialCSVReader::sniffCSV() {
    readBOM();

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
        reader = std::make_unique<SerialCSVReader>(
            readerConfig.filePaths[fileIdx], readerConfig, numColumns);
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
    auto scanInput = reinterpret_cast<function::ScanTableFuncBindInput*>(input);
    std::vector<std::string> detectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> detectedColumnTypes;
    bindColumns(scanInput->config, detectedColumnNames, detectedColumnTypes);
    std::vector<std::string> resultColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> resultColumnTypes;
    ReaderBindUtils::resolveColumns(scanInput->expectedColumnNames, detectedColumnNames,
        resultColumnNames, scanInput->expectedColumnTypes, detectedColumnTypes, resultColumnTypes);
    return std::make_unique<function::ScanBindData>(std::move(resultColumnTypes),
        std::move(resultColumnNames), scanInput->mm, scanInput->config);
}

std::unique_ptr<function::TableFuncSharedState> SerialCSVScan::initSharedState(
    function::TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<function::ScanBindData*>(input.bindData);
    common::row_idx_t numRows = 0;
    for (const auto& path : bindData->config.filePaths) {
        auto reader =
            make_unique<SerialCSVReader>(path, bindData->config, bindData->columnNames.size());
        numRows += reader->countRows();
    }
    return std::make_unique<SerialCSVScanSharedState>(
        bindData->config, numRows, bindData->columnNames.size());
}

std::unique_ptr<function::TableFuncLocalState> SerialCSVScan::initLocalState(
    function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* /*state*/) {
    return std::make_unique<function::TableFuncLocalState>();
}

void SerialCSVScan::bindColumns(const ReaderConfig& readerConfig,
    std::vector<std::string>& columnNames, std::vector<std::unique_ptr<LogicalType>>& columnTypes) {
    KU_ASSERT(readerConfig.getNumFiles() > 0);
    bindColumns(readerConfig, 0, columnNames, columnTypes);
    for (auto i = 1; i < readerConfig.getNumFiles(); ++i) {
        std::vector<std::string> tmpColumnNames;
        std::vector<std::unique_ptr<LogicalType>> tmpColumnTypes;
        bindColumns(readerConfig, i, tmpColumnNames, tmpColumnTypes);
        ReaderBindUtils::validateNumColumns(columnTypes.size(), tmpColumnTypes.size());
    }
}

void SerialCSVScan::bindColumns(const common::ReaderConfig& readerConfig, uint32_t fileIdx,
    std::vector<std::string>& columnNames,
    std::vector<std::unique_ptr<common::LogicalType>>& columnTypes) {
    auto csvReader =
        SerialCSVReader(readerConfig.filePaths[fileIdx], readerConfig, 0 /* numColumns */);
    auto sniffedColumns = csvReader.sniffCSV();
    for (auto& [name, type] : sniffedColumns) {
        columnNames.push_back(name);
        columnTypes.push_back(type.copy());
    }
}

} // namespace processor
} // namespace kuzu
