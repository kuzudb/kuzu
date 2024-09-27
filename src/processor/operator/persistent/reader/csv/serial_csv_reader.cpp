#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"

#include "function/table/bind_data.h"
#include "processor/execution_context.h"
#include "processor/operator/persistent/reader/csv/driver.h"
#include "processor/operator/persistent/reader/reader_bind_utils.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

SerialCSVReader::SerialCSVReader(const std::string& filePath, common::idx_t fileIdx,
    common::CSVOption option, CSVColumnInfo columnInfo, main::ClientContext* context,
    LocalFileErrorHandler* errorHandler, const function::ScanTableFuncBindInput* bindInput)
    : BaseCSVReader{filePath, fileIdx, std::move(option), std::move(columnInfo), context,
          errorHandler},
      bindInput{bindInput} {}

std::vector<std::pair<std::string, LogicalType>> SerialCSVReader::sniffCSV() {
    readBOM();
    SniffCSVNameAndTypeDriver driver{this, bindInput};
    parseCSV(driver);
    // finalize the columns; rename duplicate names
    std::map<std::string, int32_t> names;
    for (auto& i : driver.columns) {
        // Suppose name "col" already exists
        // Let N be the number of times it exists
        // rename to "col" + "_{N}"
        // ideally "col_{N}" shouldn't exist, but if it already exists M times (due to user
        // declaration), rename to "col_{N}" + "_{M}" repeat until no match exists
        while (names.contains(i.first)) {
            names[i.first]++;
            i.first += "_" + std::to_string(names[i.first]);
        }
        names[i.first];
        // purge null types
        i.second = LogicalTypeUtils::purgeAny(i.second, LogicalType::STRING());
    }
    return std::move(driver.columns);
}

uint64_t SerialCSVReader::parseBlock(block_idx_t blockIdx, DataChunk& resultChunk) {
    KU_ASSERT(nullptr != errorHandler);

    if (blockIdx != currentBlockIdx) {
        resetNumRowsInCurrentBlock();
    }
    currentBlockIdx = blockIdx;
    if (blockIdx == 0) {
        uint64_t headerNumRows = handleFirstBlock();
        errorHandler->setHeaderNumRows(headerNumRows);
    }
    SerialParsingDriver driver(resultChunk, this);
    auto numRowsRead = parseCSV(driver);
    errorHandler->reportFinishedBlock(blockIdx, numRowsRead);
    resultChunk.state->getSelVectorUnsafe().setSelSize(numRowsRead);
    increaseNumRowsInCurrentBlock(numRowsRead);
    return numRowsRead;
}

SerialCSVScanSharedState::SerialCSVScanSharedState(common::ReaderConfig readerConfig,
    uint64_t numRows, main::ClientContext* context, common::CSVOption csvOption,
    CSVColumnInfo columnInfo, uint64_t queryID)
    : ScanFileSharedState{std::move(readerConfig), numRows, context},
      csvOption{std::move(csvOption)}, columnInfo{std::move(columnInfo)}, totalReadSizeByFile{0},
      queryID(queryID), populateErrorFunc(constructPopulateFunc()) {
    initReader(context);
}

populate_func_t SerialCSVScanSharedState::constructPopulateFunc() {
    return [this](CopyFromFileError error, idx_t fileIdx) -> PopulatedCopyFromError {
        return BaseCSVReader::basePopulateErrorFunc(std::move(error), sharedErrorHandler.get(),
            reader.get(), readerConfig.getFilePath(fileIdx));
    };
}

void SerialCSVScanSharedState::read(DataChunk& outputChunk) {
    std::lock_guard<std::mutex> mtx{lock};
    do {
        if (fileIdx >= readerConfig.getNumFiles()) {
            return;
        }
        uint64_t numRows = reader->parseBlock(reader->getFileOffset() == 0 ? 0 : 1, outputChunk);
        if (numRows > 0) {
            return;
        }
        totalReadSizeByFile += reader->getFileSize();
        finalizeReader(context);
        fileIdx++;
        initReader(context);
    } while (true);
}

void SerialCSVScanSharedState::finalizeReader(main::ClientContext* context) const {
    if (localErrorHandler) {
        localErrorHandler->finalize();
    }
    if (sharedErrorHandler) {
        // We don't need to throw cached errors in the finalize state
        // as we only cache errors to throw when reading CSV files in parallel
        context->getWarningContextUnsafe().populateWarnings(queryID, populateErrorFunc,
            BaseCSVReader::getFileIdxFunc);
    }
}

void SerialCSVScanSharedState::initReader(main::ClientContext* context) {
    if (fileIdx < readerConfig.getNumFiles()) {
        sharedErrorHandler =
            std::make_unique<SharedFileErrorHandler>(fileIdx, nullptr, populateErrorFunc);
        localErrorHandler = std::make_unique<LocalFileErrorHandler>(sharedErrorHandler.get(),
            csvOption.ignoreErrors, context);
        reader = std::make_unique<SerialCSVReader>(readerConfig.filePaths[fileIdx], fileIdx,
            csvOption.copy(), columnInfo.copy(), context, localErrorHandler.get());
    }
}

static common::offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto serialCSVScanSharedState = ku_dynamic_cast<SerialCSVScanSharedState*>(input.sharedState);
    serialCSVScanSharedState->read(output.dataChunk);
    return output.dataChunk.state->getSelVector().getSelSize();
}

static void bindColumnsFromFile(const ScanTableFuncBindInput* bindInput, uint32_t fileIdx,
    std::vector<std::string>& columnNames, std::vector<LogicalType>& columnTypes) {
    auto csvOption = CSVReaderConfig::construct(bindInput->config.options).option;
    auto columnInfo = CSVColumnInfo(bindInput->expectedColumnNames.size() /* numColumns */,
        {} /* columnSkips */, {} /*warningDataColumns*/);
    SharedFileErrorHandler sharedErrorHandler{fileIdx, nullptr};
    // We don't want to cache CSV errors encountered during sniffing, they will be re-encountered
    // when actually parsing
    LocalFileErrorHandler errorHandler{&sharedErrorHandler, csvOption.ignoreErrors,
        bindInput->context, false};
    auto csvReader = SerialCSVReader(bindInput->config.filePaths[fileIdx], fileIdx,
        csvOption.copy(), columnInfo.copy(), bindInput->context, &errorHandler, bindInput);
    sharedErrorHandler.setPopulateErrorFunc(
        [&sharedErrorHandler, &csvReader, bindInput](CopyFromFileError error,
            idx_t fileIdx) -> PopulatedCopyFromError {
            return BaseCSVReader::basePopulateErrorFunc(std::move(error), &sharedErrorHandler,
                &csvReader, bindInput->config.filePaths[fileIdx]);
        });
    auto sniffedColumns = csvReader.sniffCSV();
    for (auto& [name, type] : sniffedColumns) {
        columnNames.push_back(name);
        columnTypes.push_back(type.copy());
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
    ScanTableFuncBindInput* scanInput) {
    if (scanInput->expectedColumnTypes.size() > 0) {
        scanInput->config.options.insert_or_assign("SAMPLE_SIZE",
            Value((int64_t)0)); // only scan headers
    }
    std::vector<std::string> detectedColumnNames;
    std::vector<LogicalType> detectedColumnTypes;
    SerialCSVScan::bindColumns(scanInput, detectedColumnNames, detectedColumnTypes);
    std::vector<std::string> resultColumnNames;
    std::vector<LogicalType> resultColumnTypes;
    ReaderBindUtils::resolveColumns(scanInput->expectedColumnNames, detectedColumnNames,
        resultColumnNames, scanInput->expectedColumnTypes, detectedColumnTypes, resultColumnTypes);

    const column_id_t numWarningDataColumns = BaseCSVReader::appendWarningDataColumns(
        resultColumnNames, resultColumnTypes, scanInput->config);

    return std::make_unique<ScanBindData>(std::move(resultColumnTypes),
        std::move(resultColumnNames), scanInput->config.copy(), scanInput->context,
        numWarningDataColumns);
}

static std::unique_ptr<TableFuncSharedState> initSharedState(TableFunctionInitInput& input) {
    auto bindData = input.bindData->constPtrCast<ScanBindData>();
    auto csvOption = CSVReaderConfig::construct(bindData->config.options).option;
    row_idx_t numRows = 0;
    auto columnInfo = CSVColumnInfo(bindData->getNumColumns() - bindData->numWarningDataColumns,
        bindData->getColumnSkips(), bindData->numWarningDataColumns);
    auto sharedState = std::make_unique<SerialCSVScanSharedState>(bindData->config.copy(), numRows,
        bindData->context, csvOption.copy(), columnInfo.copy(), input.queryID);
    for (idx_t i = 0; i < sharedState->readerConfig.filePaths.size(); ++i) {
        const auto& filePath = sharedState->readerConfig.filePaths[i];
        auto reader = std::make_unique<SerialCSVReader>(filePath, i, csvOption.copy(),
            columnInfo.copy(), sharedState->context, nullptr);
        sharedState->totalSize += reader->getFileSize();
    }
    return sharedState;
}

static std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput& /*input*/,
    TableFuncSharedState* /*state*/, storage::MemoryManager* /*mm*/) {
    return std::make_unique<TableFuncLocalState>();
}

static void finalizeFunc(ExecutionContext* ctx, TableFuncSharedState* sharedState,
    TableFuncLocalState*) {
    auto state = ku_dynamic_cast<SerialCSVScanSharedState*>(sharedState);
    state->finalizeReader(ctx->clientContext);
}

static double progressFunc(TableFuncSharedState* sharedState) {
    auto state = ku_dynamic_cast<SerialCSVScanSharedState*>(sharedState);
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
            progressFunc, std::vector<LogicalTypeID>{LogicalTypeID::STRING}, finalizeFunc));
    return functionSet;
}

} // namespace processor
} // namespace kuzu
