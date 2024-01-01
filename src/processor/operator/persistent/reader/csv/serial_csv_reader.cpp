#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"

#include "common/string_format.h"
#include "processor/operator/persistent/reader/csv/driver.h"
#include "processor/operator/persistent/reader/reader_bind_utils.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

SerialCSVReader::SerialCSVReader(const std::string& filePath, common::CSVOption option,
    uint64_t numColumns, VirtualFileSystem* vfs)
    : BaseCSVReader{filePath, std::move(option), numColumns, vfs} {}

std::vector<std::pair<std::string, LogicalType>> SerialCSVReader::sniffCSV() {
    readBOM();

    if (option.hasHeader) {
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
            readerConfig.filePaths[fileIdx], csvReaderConfig.option.copy(), numColumns, vfs);
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
    catalog::Catalog* /*catalog*/, storage::StorageManager* /*storageManager*/) {
    auto scanInput = reinterpret_cast<function::ScanTableFuncBindInput*>(input);
    std::vector<std::string> detectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> detectedColumnTypes;
    bindColumns(scanInput, detectedColumnNames, detectedColumnTypes);
    std::vector<std::string> resultColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> resultColumnTypes;
    ReaderBindUtils::resolveColumns(scanInput->expectedColumnNames, detectedColumnNames,
        resultColumnNames, scanInput->expectedColumnTypes, detectedColumnTypes, resultColumnTypes);
    return std::make_unique<function::ScanBindData>(std::move(resultColumnTypes),
        std::move(resultColumnNames), scanInput->mm, scanInput->config.copy(), scanInput->vfs);
}

std::unique_ptr<function::TableFuncSharedState> SerialCSVScan::initSharedState(
    function::TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<function::ScanBindData*>(input.bindData);
    auto csvConfig = CSVReaderConfig::construct(bindData->config.options);
    common::row_idx_t numRows = 0;
    for (const auto& path : bindData->config.filePaths) {
        auto reader = make_unique<SerialCSVReader>(
            path, csvConfig.option.copy(), bindData->columnNames.size(), bindData->vfs);
        numRows += reader->countRows();
    }
    return std::make_unique<SerialCSVScanSharedState>(bindData->config.copy(), numRows,
        bindData->columnNames.size(), bindData->vfs, csvConfig.copy());
}

std::unique_ptr<function::TableFuncLocalState> SerialCSVScan::initLocalState(
    function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* /*state*/,
    storage::MemoryManager* /*mm*/) {
    return std::make_unique<function::TableFuncLocalState>();
}

void SerialCSVScan::bindColumns(const ScanTableFuncBindInput* bindInput,
    std::vector<std::string>& columnNames, std::vector<std::unique_ptr<LogicalType>>& columnTypes) {
    KU_ASSERT(bindInput->config.getNumFiles() > 0);
    bindColumns(bindInput, 0, columnNames, columnTypes);
    for (auto i = 1u; i < bindInput->config.getNumFiles(); ++i) {
        std::vector<std::string> tmpColumnNames;
        std::vector<std::unique_ptr<LogicalType>> tmpColumnTypes;
        bindColumns(bindInput, i, tmpColumnNames, tmpColumnTypes);
        ReaderBindUtils::validateNumColumns(columnTypes.size(), tmpColumnTypes.size());
    }
}

void SerialCSVScan::bindColumns(const ScanTableFuncBindInput* bindInput, uint32_t fileIdx,
    std::vector<std::string>& columnNames,
    std::vector<std::unique_ptr<common::LogicalType>>& columnTypes) {
    auto csvConfig = CSVReaderConfig::construct(bindInput->config.options);
    auto csvReader = SerialCSVReader(bindInput->config.filePaths[fileIdx], csvConfig.option.copy(),
        0 /* numColumns */, bindInput->vfs);
    auto sniffedColumns = csvReader.sniffCSV();
    for (auto& [name, type] : sniffedColumns) {
        columnNames.push_back(name);
        columnTypes.push_back(type.copy());
    }
}

} // namespace processor
} // namespace kuzu
