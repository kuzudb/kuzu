#include "processor/operator/persistent/reader/csv/parallel_csv_reader.h"

#include "function/table/bind_data.h"
#include "processor/execution_context.h"
#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"
#include "processor/operator/persistent/reader/reader_bind_utils.h"

#if defined(_WIN32)
#include <io.h>
#endif

#include "common/string_format.h"
#include "common/system_message.h"
#include "processor/operator/persistent/reader/csv/driver.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

ParallelCSVReader::ParallelCSVReader(const std::string& filePath, common::idx_t fileIdx,
    CSVOption option, CSVColumnInfo columnInfo, main::ClientContext* context,
    LocalFileErrorHandler* errorHandler)
    : BaseCSVReader{filePath, fileIdx, std::move(option), std::move(columnInfo), context,
          errorHandler} {}

bool ParallelCSVReader::hasMoreToRead() const {
    // If we haven't started the first block yet or are done our block, get the next block.
    return buffer != nullptr && !finishedBlock();
}

uint64_t ParallelCSVReader::parseBlock(block_idx_t blockIdx, DataChunk& resultChunk) {
    currentBlockIdx = blockIdx;
    resetNumRowsInCurrentBlock();
    seekToBlockStart();
    if (blockIdx == 0) {
        readBOM();
        if (option.hasHeader) {
            uint64_t headerNumRows = readHeader();
            errorHandler->setHeaderNumRows(headerNumRows);
        }
    }
    if (finishedBlock()) {
        return 0;
    }
    ParallelParsingDriver driver(resultChunk, this);
    const auto numRowsParsed = parseCSV(driver);
    increaseNumRowsInCurrentBlock(numRowsParsed);
    return numRowsParsed;
}

void ParallelCSVReader::reportFinishedBlock() {
    errorHandler->reportFinishedBlock(currentBlockIdx, getNumRowsInCurrentBlock());
}

uint64_t ParallelCSVReader::continueBlock(DataChunk& resultChunk) {
    KU_ASSERT(hasMoreToRead());
    ParallelParsingDriver driver(resultChunk, this);
    const auto numRowsParsed = parseCSV(driver);
    increaseNumRowsInCurrentBlock(numRowsParsed);
    return numRowsParsed;
}

void ParallelCSVReader::seekToBlockStart() {
    // Seek to the proper location in the file.
    if (fileInfo->seek(currentBlockIdx * CopyConstants::PARALLEL_BLOCK_SIZE, SEEK_SET) == -1) {
        // LCOV_EXCL_START
        handleCopyException(
            stringFormat("Failed to seek to block {}: {}", currentBlockIdx, posixErrMessage()),
            true);
        // LCOV_EXCL_STOP
    }
    osFileOffset = currentBlockIdx * CopyConstants::PARALLEL_BLOCK_SIZE;

    if (currentBlockIdx == 0) {
        // First block doesn't search for a newline.
        return;
    }

    // Reset the buffer.
    position = 0;
    bufferSize = 0;
    buffer.reset();
    if (!readBuffer(nullptr)) {
        return;
    }

    // Find the start of the next line.
    do {
        for (; position < bufferSize; position++) {
            if (buffer[position] == '\r') {
                position++;
                if (!maybeReadBuffer(nullptr)) {
                    return;
                }
                if (buffer[position] == '\n') {
                    position++;
                }
                return;
            } else if (buffer[position] == '\n') {
                position++;
                return;
            }
        }
    } while (readBuffer(nullptr));
}

bool ParallelCSVReader::handleQuotedNewline() {
    lineContext.setEndOfLine(getFileOffset());
    handleCopyException("Quoted newlines are not supported in parallel CSV reader."
                        " Please specify PARALLEL=FALSE in the options.");
    return false;
}

bool ParallelCSVReader::finishedBlock() const {
    // Only stop if we've ventured into the next block by at least a byte.
    // Use `>` because `position` points to just past the newline right now.
    return getFileOffset() > (currentBlockIdx + 1) * CopyConstants::PARALLEL_BLOCK_SIZE;
}

ParallelCSVScanSharedState::ParallelCSVScanSharedState(common::ReaderConfig readerConfig,
    uint64_t numRows, main::ClientContext* context, common::CSVOption csvOption,
    CSVColumnInfo columnInfo)
    : ScanFileSharedState{std::move(readerConfig), numRows, context},
      csvOption{std::move(csvOption)}, columnInfo{std::move(columnInfo)}, numBlocksReadByFiles{0} {
    errorHandlers.reserve(this->readerConfig.getNumFiles());
    for (idx_t i = 0; i < this->readerConfig.getNumFiles(); ++i) {
        errorHandlers.emplace_back(i, &lock);
    }
    populateErrorFunc = constructPopulateFunc();
    for (auto& errorHandler : errorHandlers) {
        errorHandler.setPopulateErrorFunc(populateErrorFunc);
    }
}

populate_func_t ParallelCSVScanSharedState::constructPopulateFunc() {
    const auto numFiles = readerConfig.getNumFiles();
    auto localErrorHandlers = std::vector<std::shared_ptr<LocalFileErrorHandler>>(numFiles);
    auto readers = std::vector<std::shared_ptr<SerialCSVReader>>(numFiles);
    for (idx_t i = 0; i < numFiles; ++i) {
        // If we run into errors while reconstructing lines they should be unrecoverable
        localErrorHandlers[i] =
            std::make_shared<LocalFileErrorHandler>(&errorHandlers[i], false, context);
        readers[i] = std::make_shared<SerialCSVReader>(readerConfig.filePaths[i], i,
            csvOption.copy(), columnInfo.copy(), context, localErrorHandlers[i].get());
    }
    return [this, movedErrorHandlers = std::move(localErrorHandlers),
               movedReaders = std::move(readers)](CopyFromFileError error,
               common::idx_t fileIdx) -> PopulatedCopyFromError {
        return BaseCSVReader::basePopulateErrorFunc(std::move(error), &errorHandlers[fileIdx],
            movedReaders[fileIdx].get(), readerConfig.getFilePath(fileIdx));
    };
}

void ParallelCSVScanSharedState::setFileComplete(uint64_t completedFileIdx) {
    std::lock_guard<std::mutex> guard{lock};
    if (completedFileIdx == fileIdx) {
        numBlocksReadByFiles += blockIdx;
        blockIdx = 0;
        fileIdx++;
    }
}

static offset_t tableFunc(TableFuncInput& input, TableFuncOutput& output) {
    auto& outputChunk = output.dataChunk;

    auto localState = input.localState->ptrCast<ParallelCSVLocalState>();
    auto sharedState = input.sharedState->ptrCast<ParallelCSVScanSharedState>();

    do {
        if (localState->reader != nullptr) {
            if (localState->reader->hasMoreToRead()) {
                auto result = localState->reader->continueBlock(outputChunk);
                outputChunk.state->getSelVectorUnsafe().setSelSize(result);
                if (result > 0) {
                    return result;
                }
            }
            localState->reader->reportFinishedBlock();
        }
        auto [fileIdx, blockIdx] = sharedState->getNext();
        if (fileIdx == UINT64_MAX) {
            return 0;
        }
        if (fileIdx != localState->fileIdx) {
            localState->fileIdx = fileIdx;
            localState->errorHandler =
                std::make_unique<LocalFileErrorHandler>(&sharedState->errorHandlers[fileIdx],
                    sharedState->csvOption.ignoreErrors, sharedState->context, true);
            localState->reader =
                std::make_unique<ParallelCSVReader>(sharedState->readerConfig.filePaths[fileIdx],
                    fileIdx, sharedState->csvOption.copy(), sharedState->columnInfo.copy(),
                    sharedState->context, localState->errorHandler.get());
        }
        auto numRowsRead = localState->reader->parseBlock(blockIdx, outputChunk);

        // if there are any pending errors to throw, stop the parsing
        // the actual error will be thrown during finalize
        if (!sharedState->csvOption.ignoreErrors &&
            sharedState->errorHandlers[fileIdx].getNumCachedErrors() > 0) {
            numRowsRead = 0;
        }

        outputChunk.state->getSelVectorUnsafe().setSelSize(numRowsRead);
        if (numRowsRead > 0) {
            return numRowsRead;
        }
        if (localState->reader->isEOF()) {
            localState->reader->reportFinishedBlock();
            localState->errorHandler->finalize();
            sharedState->setFileComplete(localState->fileIdx);
            localState->reader = nullptr;
            localState->errorHandler = nullptr;
        }
    } while (true);
}

static std::unique_ptr<TableFuncBindData> bindFunc(main::ClientContext* /*context*/,
    ScanTableFuncBindInput* scanInput) {
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

    if (csvOption.autoDetection) {
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
    auto sharedState = std::make_unique<ParallelCSVScanSharedState>(bindData->config.copy(),
        numRows, bindData->context, csvOption.copy(), columnInfo.copy());

    for (idx_t i = 0; i < sharedState->readerConfig.getNumFiles(); ++i) {
        auto filePath = sharedState->readerConfig.filePaths[i];
        auto reader = std::make_unique<ParallelCSVReader>(filePath, i, csvOption.copy(),
            columnInfo.copy(), bindData->context, nullptr);
        sharedState->totalSize += reader->getFileSize();
    }

    return sharedState;
}

static std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput& /*input*/,
    TableFuncSharedState* /*state*/, storage::MemoryManager* /*mm*/) {
    auto localState = std::make_unique<ParallelCSVLocalState>();
    localState->fileIdx = std::numeric_limits<decltype(localState->fileIdx)>::max();

    return localState;
}

static double progressFunc(TableFuncSharedState* sharedState) {
    auto state = sharedState->ptrCast<ParallelCSVScanSharedState>();
    if (state->fileIdx >= state->readerConfig.getNumFiles()) {
        return 1.0;
    }
    if (state->totalSize == 0) {
        return 0.0;
    }
    uint64_t totalReadSize =
        (state->numBlocksReadByFiles + state->blockIdx) * CopyConstants::PARALLEL_BLOCK_SIZE;
    if (totalReadSize > state->totalSize) {
        return 1.0;
    }
    return static_cast<double>(totalReadSize) / state->totalSize;
}

static void finalizeFunc(ExecutionContext* ctx, TableFuncSharedState* sharedState,
    TableFuncLocalState* /*localState*/) {
    auto state = ku_dynamic_cast<ParallelCSVScanSharedState*>(sharedState);
    for (idx_t i = 0; i < state->readerConfig.getNumFiles(); ++i) {
        state->errorHandlers[i].throwCachedErrorsIfNeeded();
    }
    ctx->clientContext->getWarningContextUnsafe().populateWarnings(ctx->queryID,
        state->populateErrorFunc, BaseCSVReader::getFileIdxFunc);
}

function_set ParallelCSVScan::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState, initLocalState,
            progressFunc, std::vector<LogicalTypeID>{LogicalTypeID::STRING}, finalizeFunc));
    return functionSet;
}

} // namespace processor
} // namespace kuzu
