#include "processor/operator/persistent/reader/csv/parallel_csv_reader.h"

#include "function/table/bind_data.h"
#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"
#include "processor/operator/persistent/reader/reader_bind_utils.h"

#if defined(_WIN32)
#include <io.h>
#endif

#include "common/exception/copy.h"
#include "common/string_format.h"
#include "common/system_message.h"
#include "processor/operator/persistent/reader/csv/driver.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

ParallelCSVReader::ParallelCSVReader(const std::string& filePath, CSVOption option,
    uint64_t numColumns, main::ClientContext* context)
    : BaseCSVReader{filePath, std::move(option), numColumns, context} {}

bool ParallelCSVReader::hasMoreToRead() const {
    // If we haven't started the first block yet or are done our block, get the next block.
    return buffer != nullptr && !finishedBlock();
}

uint64_t ParallelCSVReader::parseBlock(block_idx_t blockIdx, DataChunk& resultChunk) {
    currentBlockIdx = blockIdx;
    seekToBlockStart();
    if (blockIdx == 0) {
        readBOM();
        if (option.hasHeader) {
            readHeader();
        }
    }
    if (finishedBlock()) {
        return 0;
    }
    ParallelParsingDriver driver(resultChunk, this);
    return parseCSV(driver);
}

uint64_t ParallelCSVReader::continueBlock(DataChunk& resultChunk) {
    KU_ASSERT(hasMoreToRead());
    ParallelParsingDriver driver(resultChunk, this);
    return parseCSV(driver);
}

void ParallelCSVReader::seekToBlockStart() {
    // Seek to the proper location in the file.
    if (fileInfo->seek(currentBlockIdx * CopyConstants::PARALLEL_BLOCK_SIZE, SEEK_SET) == -1) {
        // LCOV_EXCL_START
        throw CopyException(stringFormat("Failed to seek to block {} in file {}: {}",
            currentBlockIdx, fileInfo->path, posixErrMessage()));
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

void ParallelCSVReader::handleQuotedNewline() {
    throw CopyException(stringFormat("Quoted newlines are not supported in parallel CSV reader "
                                     "(while parsing {} on line {}). Please "
                                     "specify PARALLEL=FALSE in the options.",
        fileInfo->path, getLineNumber()));
}

bool ParallelCSVReader::finishedBlock() const {
    // Only stop if we've ventured into the next block by at least a byte.
    // Use `>` because `position` points to just past the newline right now.
    return getFileOffset() > (currentBlockIdx + 1) * CopyConstants::PARALLEL_BLOCK_SIZE;
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
    auto parallelCSVLocalState =
        ku_dynamic_cast<TableFuncLocalState*, ParallelCSVLocalState*>(input.localState);
    auto parallelCSVSharedState =
        ku_dynamic_cast<TableFuncSharedState*, ParallelCSVScanSharedState*>(input.sharedState);
    do {
        if (parallelCSVLocalState->reader != nullptr &&
            parallelCSVLocalState->reader->hasMoreToRead()) {
            auto result = parallelCSVLocalState->reader->continueBlock(outputChunk);
            outputChunk.state->getSelVectorUnsafe().setSelSize(result);
            if (result > 0) {
                return result;
            }
        }
        auto [fileIdx, blockIdx] = parallelCSVSharedState->getNext();
        if (fileIdx == UINT64_MAX) {
            return 0;
        }
        if (fileIdx != parallelCSVLocalState->fileIdx) {
            parallelCSVLocalState->fileIdx = fileIdx;
            parallelCSVLocalState->reader = std::make_unique<ParallelCSVReader>(
                parallelCSVSharedState->readerConfig.filePaths[fileIdx],
                parallelCSVSharedState->csvReaderConfig.option.copy(),
                parallelCSVSharedState->numColumns, parallelCSVSharedState->context);
        }
        auto numRowsRead = parallelCSVLocalState->reader->parseBlock(blockIdx, outputChunk);
        outputChunk.state->getSelVectorUnsafe().setSelSize(numRowsRead);
        if (numRowsRead > 0) {
            return numRowsRead;
        }
        if (parallelCSVLocalState->reader->isEOF()) {
            parallelCSVSharedState->setFileComplete(parallelCSVLocalState->fileIdx);
            parallelCSVLocalState->reader = nullptr;
        }
    } while (true);
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
    auto sharedState = std::make_unique<ParallelCSVScanSharedState>(bindData->config.copy(),
        numRows, bindData->columnNames.size(), bindData->context, csvConfig.copy());
    for (auto filePath : sharedState->readerConfig.filePaths) {
        auto reader = std::make_unique<ParallelCSVReader>(filePath,
            sharedState->csvReaderConfig.option.copy(), sharedState->numColumns,
            sharedState->context);
        sharedState->totalSize += reader->getFileSize();
    }
    return sharedState;
}

static std::unique_ptr<TableFuncLocalState> initLocalState(TableFunctionInitInput& /*input*/,
    TableFuncSharedState* state, storage::MemoryManager* /*mm*/) {
    auto localState = std::make_unique<ParallelCSVLocalState>();
    auto sharedState = ku_dynamic_cast<TableFuncSharedState*, ParallelCSVScanSharedState*>(state);
    localState->reader = std::make_unique<ParallelCSVReader>(sharedState->readerConfig.filePaths[0],
        sharedState->csvReaderConfig.option.copy(), sharedState->numColumns, sharedState->context);
    localState->fileIdx = 0;
    return localState;
}

static double progressFunc(TableFuncSharedState* sharedState) {
    auto state = ku_dynamic_cast<TableFuncSharedState*, ParallelCSVScanSharedState*>(sharedState);
    if (state->fileIdx >= state->readerConfig.getNumFiles()) {
        return 1.0;
    }
    if (state->totalSize == 0) {
        return 0.0;
    }
    uint64_t totalReadSize =
        (state->numBlocksReadByFiles + state->blockIdx) * CopyConstants::PARALLEL_BLOCK_SIZE;
    return static_cast<double>(totalReadSize) / state->totalSize;
}

function_set ParallelCSVScan::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<TableFunction>(name, tableFunc, bindFunc, initSharedState, initLocalState,
            progressFunc, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

} // namespace processor
} // namespace kuzu
