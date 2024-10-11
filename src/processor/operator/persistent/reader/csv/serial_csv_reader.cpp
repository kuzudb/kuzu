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

std::vector<std::pair<std::string, LogicalType>> SerialCSVReader::sniffCSV(
    DialectOption& detectedDialect, bool& detectedHeader) {
    auto csvOption = CSVReaderConfig::construct(bindInput->config.options).option;
    readBOM();

    if (detectedDialect.doDialectDetection) {
        detectedDialect = detectDialect();
    }

    SniffCSVNameAndTypeDriver driver{this, bindInput};
    parseCSV(driver);

    for (auto& i : driver.columns) {
        // purge null types
        i.second = LogicalTypeUtils::purgeAny(i.second, LogicalType::STRING());
    }
    // Do header detection IFF user didn't set header AND user didn't turn off auto detection
    if (!csvOption.setHeader && csvOption.autoDetection) {
        detectedHeader = detectHeader(driver.columns);
    }

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
    std::vector<std::string>& columnNames, std::vector<LogicalType>& columnTypes,
    DialectOption& detectedDialect, bool& detectedHeader) {
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
    auto sniffedColumns = csvReader.sniffCSV(detectedDialect, detectedHeader);
    for (auto& [name, type] : sniffedColumns) {
        columnNames.push_back(name);
        columnTypes.push_back(type.copy());
    }
}

void SerialCSVScan::bindColumns(const ScanTableFuncBindInput* bindInput,
    std::vector<std::string>& columnNames, std::vector<LogicalType>& columnTypes,
    DialectOption& detectedDialect, bool& detectedHeader) {
    KU_ASSERT(bindInput->config.getNumFiles() > 0);
    bindColumnsFromFile(bindInput, 0, columnNames, columnTypes, detectedDialect, detectedHeader);
    for (auto i = 1u; i < bindInput->config.getNumFiles(); ++i) {
        std::vector<std::string> tmpColumnNames;
        std::vector<LogicalType> tmpColumnTypes;
        bindColumnsFromFile(bindInput, i, tmpColumnNames, tmpColumnTypes, detectedDialect,
            detectedHeader);
        ReaderBindUtils::validateNumColumns(columnTypes.size(), tmpColumnTypes.size());
    }
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
    ScanTableFuncBindInput* scanInput) {
    if (scanInput->expectedColumnTypes.size() > 0) {
        scanInput->config.options.insert_or_assign("SAMPLE_SIZE",
            Value((int64_t)0)); // only scan headers
    }

    bool detectedHeader = false;

    DialectOption detectedDialect;
    auto csvOption = CSVReaderConfig::construct(scanInput->config.options).option;
    detectedDialect.doDialectDetection = csvOption.autoDetection;

    std::vector<std::string> detectedColumnNames;
    std::vector<LogicalType> detectedColumnTypes;
    SerialCSVScan::bindColumns(scanInput, detectedColumnNames, detectedColumnTypes, detectedDialect,
        detectedHeader);

    std::vector<std::string> resultColumnNames;
    std::vector<LogicalType> resultColumnTypes;
    ReaderBindUtils::resolveColumns(scanInput->expectedColumnNames, detectedColumnNames,
        resultColumnNames, scanInput->expectedColumnTypes, detectedColumnTypes, resultColumnTypes);

    if (detectedDialect.doDialectDetection) {
        std::string quote(1, detectedDialect.quoteChar);
        std::string delim(1, detectedDialect.delimiter);
        std::string escape(1, detectedDialect.escapeChar);
        scanInput->config.options.insert_or_assign("ESCAPE", Value(LogicalType::STRING(), escape));
        scanInput->config.options.insert_or_assign("QUOTE", Value(LogicalType::STRING(), quote));
        scanInput->config.options.insert_or_assign("DELIM", Value(LogicalType::STRING(), delim));
    }

    if (!csvOption.setHeader && csvOption.autoDetection && detectedHeader) {
        scanInput->config.options.insert_or_assign("HEADER", Value(detectedHeader));
    }

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

void SerialCSVReader::resetReaderState() {
    // Reset file position to the beginning.
    if (-1 == fileInfo->seek(0, SEEK_SET)) {
        handleCopyException(
            stringFormat("Failed to seek to the beginning of the file:, errno: {}.", errno), true);
        return;
    }

    buffer.reset();
    bufferSize = 0;
    position = 0;
    osFileOffset = 0;
    bufferIdx = 0;
    lineContext.setNewLine(getFileOffset());

    readBOM();
}

DialectOption SerialCSVReader::detectDialect() {
    // Extract a sample of rows from the file for dialect detection.
    SniffCSVDialectDriver driver{this, bindInput};

    // Generate dialect options based on the non-user-specified options.
    auto dialectSearchSpace = generateDialectOptions(option);

    // Save default for dialect not found situation.
    DialectOption defaultOption{option.delimiter, option.quoteChar, option.escapeChar};

    idx_t bestConsistentRows = 0;
    idx_t maxColumnsFound = 0;
    idx_t minIgnoredRows = 0;
    std::vector<DialectOption> validDialects;
    std::vector<DialectOption> finalDialects;
    for (auto& dialectOption : dialectSearchSpace) {
        bool notExpected = false;
        // Load current dialect option.
        option.delimiter = dialectOption.delimiter;
        option.quoteChar = dialectOption.quoteChar;
        option.escapeChar = dialectOption.escapeChar;
        // reset Driver.
        driver.reset();
        // Try parsing it with current dialect.
        parseCSV(driver);
        // Reset the file position and buffer to start reading from the beginning after detection.
        resetReaderState();
        // If never unquoting quoted values or any other error during the parsing, discard this
        // dialect.
        if (driver.getError()) {
            continue;
        }

        idx_t ignoredRows = 0;
        idx_t consistentRows = 0;
        idx_t numCols = driver.getResultPosition() == 0 ? 1 : driver.getColumnCount(0);
        dialectOption.everQuoted = driver.getEverQuoted();
        dialectOption.everEscaped = driver.getEverEscaped();

        // If the columns didn't match the user input columns number.
        if (getNumColumns() != 0 && getNumColumns() != numCols) {
            continue;
        }

        for (auto row = 0u; row < driver.getResultPosition(); row++) {
            if (getNumColumns() != 0 && getNumColumns() != driver.getColumnCount(row)) {
                notExpected = true;
                break;
            }
            if (numCols < driver.getColumnCount(row)) {
                numCols = driver.getColumnCount(row);
                consistentRows = 1;
            } else if (driver.getColumnCount(row) == numCols) {
                consistentRows++;
            } else {
                ignoredRows++;
            }
        }

        if (notExpected) {
            continue;
        }

        auto moreValues = consistentRows > bestConsistentRows && numCols >= maxColumnsFound;
        auto singleColumnBefore =
            maxColumnsFound < 2 && numCols > maxColumnsFound * validDialects.size();
        auto moreThanOneRow = consistentRows > 1;
        auto moreThanOneColumn = numCols > 1;

        if (singleColumnBefore || moreValues || moreThanOneColumn) {
            if (maxColumnsFound == numCols && ignoredRows > minIgnoredRows) {
                continue;
            }
            if (!validDialects.empty() && validDialects.front().everQuoted &&
                !dialectOption.everQuoted) {
                // Give preference to quoted dialect.
                continue;
            }

            if (!validDialects.empty() && validDialects.front().everEscaped &&
                !dialectOption.everEscaped) {
                // Give preference to Escaped dialect.
                continue;
            }

            if (consistentRows >= bestConsistentRows) {
                bestConsistentRows = consistentRows;
                maxColumnsFound = numCols;
                minIgnoredRows = ignoredRows;
                validDialects.clear();
                validDialects.emplace_back(dialectOption);
            }
        }

        if (moreThanOneRow && moreThanOneColumn && numCols == maxColumnsFound) {
            bool same_quote = false;
            for (auto& validDialect : validDialects) {
                if (validDialect.quoteChar == dialectOption.quoteChar) {
                    same_quote = true;
                }
            }

            if (!same_quote) {
                validDialects.push_back(std::move(dialectOption));
            }
        }
    }

    // If we have multiple validDialect with quotes set, we will give the preference to ones
    // that have actually quoted values.
    if (!validDialects.empty()) {
        for (auto& validDialect : validDialects) {
            if (validDialect.everQuoted) {
                finalDialects.clear();
                finalDialects.emplace_back(validDialect);
                break;
            }
            finalDialects.emplace_back(validDialect);
        }
    }

    // If the Dialect we found doesn't need Quote, we use empty as QuoteChar.
    if (!finalDialects.empty() && !finalDialects[0].everQuoted && !option.setQuote) {
        finalDialects[0].quoteChar = '\0';
    }
    // If the Dialect we found doesn't need Escape, we use empty as EscapeChar.
    if (!finalDialects.empty() && !finalDialects[0].everEscaped && !option.setEscape) {
        finalDialects[0].escapeChar = '\0';
    }

    // Apply the detected dialect to the CSV options.
    if (!finalDialects.empty()) {
        option.delimiter = finalDialects[0].delimiter;
        option.quoteChar = finalDialects[0].quoteChar;
        option.escapeChar = finalDialects[0].escapeChar;
    } else {
        option.delimiter = defaultOption.delimiter;
        option.quoteChar = defaultOption.quoteChar;
        option.escapeChar = defaultOption.escapeChar;
    }

    DialectOption ret{option.delimiter, option.quoteChar, option.escapeChar};
    return ret;
}

bool SerialCSVReader::detectHeader(
    std::vector<std::pair<std::string, common::LogicalType>>& detectedTypes) {
    // Reset the file position and buffer to start reading from the beginning after detection.
    resetReaderState();
    SniffCSVHeaderDriver sniffHeaderDriver{this, bindInput, detectedTypes};
    readBOM();
    parseCSV(sniffHeaderDriver);
    resetReaderState();
    // In this case, User didn't set Header, but we detected a Header, use the detected header to
    // set the name and type.
    if (sniffHeaderDriver.detectedHeader) {
        for (auto i = 0u; i < detectedTypes.size(); i++) {
            detectedTypes[i].first = sniffHeaderDriver.header[i].first;
        }
    }
    return sniffHeaderDriver.detectedHeader;
}

} // namespace processor
} // namespace kuzu
