#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"

#include "common/string_format.h"
#include "function/table/bind_data.h"
#include "processor/operator/persistent/reader/csv/driver.h"
#include "processor/operator/persistent/reader/reader_bind_utils.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

SerialCSVReader::SerialCSVReader(const std::string& filePath, CSVOption option, uint64_t numColumns,
    main::ClientContext* context)
    : BaseCSVReader{filePath, std::move(option), numColumns, context} {}

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

uint64_t SerialCSVReader::parseBlock(block_idx_t blockIdx, DataChunk& resultChunk) {
    currentBlockIdx = blockIdx;
    if (blockIdx == 0) {
        handleFirstBlock();
    }
    SerialParsingDriver driver(resultChunk, this);
    auto numRowsRead = parseCSV(driver);
    resultChunk.state->getSelVectorUnsafe().setSelSize(numRowsRead);
    return numRowsRead;
}

void SerialCSVScanSharedState::read(DataChunk& outputChunk) {
    std::lock_guard<std::mutex> mtx{lock};
    do {
        if (fileIdx > readerConfig.getNumFiles()) {
            return;
        }
        uint64_t numRows = reader->parseBlock(reader->getFileOffset() == 0 ? 0 : 1, outputChunk);
        if (numRows > 0) {
            return;
        }
        totalReadSizeByFile += reader->getFileSize();
        fileIdx++;
        initReader(context);
    } while (true);
}

void SerialCSVScanSharedState::initReader(main::ClientContext* context) {
    if (fileIdx < readerConfig.getNumFiles()) {
        reader = std::make_unique<SerialCSVReader>(readerConfig.filePaths[fileIdx],
            csvReaderConfig.option.copy(), numColumns, context);
    }
}

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto serialCSVScanSharedState =
        ku_dynamic_cast<TableFuncSharedState*, SerialCSVScanSharedState*>(input.sharedState);
    serialCSVScanSharedState->read(output.dataChunk);
    return output.dataChunk.state->getSelVector().getSelSize();
}

static void bindColumnsFromFile(const ScanTableFuncBindInput* bindInput, uint32_t fileIdx,
    std::vector<std::string>& columnNames, std::vector<LogicalType>& columnTypes) {
    auto csvConfig = CSVReaderConfig::construct(bindInput->config.options);
    auto csvReader = SerialCSVReader(bindInput->config.filePaths[fileIdx], csvConfig.option.copy(),
        0 /* numColumns */, bindInput->context);
    auto sniffedColumns = csvReader.sniffCSV();
    for (auto& [name, type] : sniffedColumns) {
        columnNames.push_back(name);
        columnTypes.push_back(type);
    }
}

void SerialCSVScan::bindColumns(const ScanTableFuncBindInput* bindInput,
    std::vector<std::string>& columnNames, std::vector<LogicalType>& columnTypes) {
    KU_ASSERT(bindInput->config.getNumFiles() > 0);
    bindColumnsFromFile(bindInput, 0, columnNames, columnTypes);
    for (auto i = 1u; i < bindInput->config.getNumFiles(); ++i) {
        std::vector<std::string> tmpColumnNames;
        std::vector<LogicalType> tmpColumnTypes;
        bindColumnsFromFile(bindInput, i, tmpColumnNames, tmpColumnTypes);
        ReaderBindUtils::validateNumColumns(columnTypes.size(), tmpColumnTypes.size());
    }
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
    TableFuncBindInput* input) {
    auto scanInput = ku_dynamic_cast<TableFuncBindInput*, ScanTableFuncBindInput*>(input);
    std::vector<std::string> detectedColumnNames;
    std::vector<LogicalType> detectedColumnTypes;
    SerialCSVScan::bindColumns(scanInput, detectedColumnNames, detectedColumnTypes);
    std::vector<std::string> resultColumnNames;
    std::vector<LogicalType> resultColumnTypes;
    ReaderBindUtils::resolveColumns(scanInput->expectedColumnNames, detectedColumnNames,
        resultColumnNames, scanInput->expectedColumnTypes, detectedColumnTypes, resultColumnTypes);
    return std::make_unique<ScanBindData>(std::move(resultColumnTypes),
        std::move(resultColumnNames), scanInput->config.copy(), scanInput->context);
}

static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input) {
    auto bindData = ku_dynamic_cast<TableFuncBindData*, ScanBindData*>(input.bindData);
    auto csvConfig = CSVReaderConfig::construct(bindData->config.options);
    row_idx_t numRows = 0;
    auto sharedState = std::make_unique<SerialCSVScanSharedState>(bindData->config.copy(), numRows,
        bindData->columnNames.size(), csvConfig.copy(), bindData->context);
    for (auto filePath : sharedState->readerConfig.filePaths) {
        auto reader =
            std::make_unique<SerialCSVReader>(filePath, sharedState->csvReaderConfig.option.copy(),
                sharedState->numColumns, sharedState->context);
        sharedState->totalSize += reader->getFileSize();
    }
    return sharedState;
}

static std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput& /*input*/,
    TableFuncSharedState* /*state*/, storage::MemoryManager* /*mm*/) {
    return std::make_unique<TableFuncLocalState>();
}

static double progressFunc(TableFuncSharedState* sharedState) {
    auto state = ku_dynamic_cast<TableFuncSharedState*, SerialCSVScanSharedState*>(sharedState);
    if (state->totalSize == 0) {
        return 0.0;
    } else if (state->fileIdx >= state->readerConfig.getNumFiles()) {
        return 1.0;
    }
    uint64_t totalReadSize = state->totalReadSizeByFile + state->reader->getFileOffset();
    return static_cast<double>(totalReadSize) / state->totalSize;
}

function_set SerialCSVScan::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState, initLocalState,
            progressFunc, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

} // namespace processor
} // namespace kuzu
