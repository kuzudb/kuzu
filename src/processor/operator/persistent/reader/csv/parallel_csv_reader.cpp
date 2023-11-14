#include "processor/operator/persistent/reader/csv/parallel_csv_reader.h"

#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"
#include "processor/operator/persistent/reader/reader_bind_utils.h"

#if defined(_WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif

#include "common/exception/copy.h"
#include "common/string_format.h"
#include "common/system_message.h"
#include "processor/operator/persistent/reader/csv/driver.h"

using namespace kuzu::common;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

ParallelCSVReader::ParallelCSVReader(
    const std::string& filePath, const common::ReaderConfig& readerConfig, uint64_t numColumns)
    : BaseCSVReader{filePath, readerConfig, numColumns} {}

bool ParallelCSVReader::hasMoreToRead() const {
    // If we haven't started the first block yet or are done our block, get the next block.
    return buffer != nullptr && !finishedBlock();
}

uint64_t ParallelCSVReader::parseBlock(
    common::block_idx_t blockIdx, common::DataChunk& resultChunk) {
    currentBlockIdx = blockIdx;
    seekToBlockStart();
    if (blockIdx == 0) {
        readBOM();
        if (csvReaderConfig.hasHeader) {
            readHeader();
        }
    }
    if (finishedBlock()) {
        return 0;
    }
    ParallelParsingDriver driver(resultChunk, this);
    return parseCSV(driver);
}

uint64_t ParallelCSVReader::continueBlock(common::DataChunk& resultChunk) {
    KU_ASSERT(hasMoreToRead());
    ParallelParsingDriver driver(resultChunk, this);
    return parseCSV(driver);
}

void ParallelCSVReader::seekToBlockStart() {
    // Seek to the proper location in the file.
    if (lseek(fd, currentBlockIdx * CopyConstants::PARALLEL_BLOCK_SIZE, SEEK_SET) == -1) {
        // LCOV_EXCL_START
        throw CopyException(stringFormat("Failed to seek to block {} in file {}: {}",
            currentBlockIdx, filePath, posixErrMessage()));
        // LCOV_EXCL_STOP
    }

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
        filePath, getLineNumber()));
}

bool ParallelCSVReader::finishedBlock() const {
    // Only stop if we've ventured into the next block by at least a byte.
    // Use `>` because `position` points to just past the newline right now.
    return getFileOffset() > (currentBlockIdx + 1) * CopyConstants::PARALLEL_BLOCK_SIZE;
}

void ParallelCSVScanSharedState::setFileComplete(uint64_t completedFileIdx) {
    std::lock_guard<std::mutex> guard{lock};
    if (completedFileIdx == fileIdx) {
        blockIdx = 0;
        fileIdx++;
    }
}

function::function_set ParallelCSVScan::getFunctionSet() {
    function_set functionSet;
    functionSet.push_back(
        std::make_unique<TableFunction>(READ_CSV_PARALLEL_FUNC_NAME, tableFunc, bindFunc,
            initSharedState, initLocalState, std::vector<LogicalTypeID>{LogicalTypeID::STRING}));
    return functionSet;
}

void ParallelCSVScan::tableFunc(TableFunctionInput& input, common::DataChunk& outputChunk) {
    auto parallelCSVLocalState = reinterpret_cast<ParallelCSVLocalState*>(input.localState);
    auto parallelCSVSharedState = reinterpret_cast<ParallelCSVScanSharedState*>(input.sharedState);
    do {
        if (parallelCSVLocalState->reader != nullptr &&
            parallelCSVLocalState->reader->hasMoreToRead()) {
            auto result = parallelCSVLocalState->reader->continueBlock(outputChunk);
            outputChunk.state->selVector->selectedSize = result;
            if (result > 0) {
                return;
            }
        }
        auto [fileIdx, blockIdx] = parallelCSVSharedState->getNext();
        if (fileIdx == UINT64_MAX) {
            return;
        }
        if (fileIdx != parallelCSVLocalState->fileIdx) {
            parallelCSVLocalState->fileIdx = fileIdx;
            parallelCSVLocalState->reader = std::make_unique<ParallelCSVReader>(
                parallelCSVSharedState->readerConfig.filePaths[fileIdx],
                parallelCSVSharedState->readerConfig, parallelCSVSharedState->numColumns);
        }
        auto numRowsRead = parallelCSVLocalState->reader->parseBlock(blockIdx, outputChunk);
        outputChunk.state->selVector->selectedSize = numRowsRead;
        if (numRowsRead > 0) {
            return;
        }
        if (parallelCSVLocalState->reader->isEOF()) {
            parallelCSVSharedState->setFileComplete(parallelCSVLocalState->fileIdx);
            parallelCSVLocalState->reader = nullptr;
        }
    } while (true);
}

std::unique_ptr<function::TableFuncBindData> ParallelCSVScan::bindFunc(
    main::ClientContext* /*context*/, function::TableFuncBindInput* input,
    catalog::CatalogContent* /*catalog*/) {
    auto scanInput = reinterpret_cast<function::ScanTableFuncBindInput*>(input);
    std::vector<std::string> detectedColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> detectedColumnTypes;
    SerialCSVScan::bindColumns(scanInput->config, detectedColumnNames, detectedColumnTypes);
    std::vector<std::string> resultColumnNames;
    std::vector<std::unique_ptr<common::LogicalType>> resultColumnTypes;
    ReaderBindUtils::resolveColumns(scanInput->expectedColumnNames, detectedColumnNames,
        resultColumnNames, scanInput->expectedColumnTypes, detectedColumnTypes, resultColumnTypes);
    return std::make_unique<function::ScanBindData>(std::move(resultColumnTypes),
        std::move(resultColumnNames), scanInput->mm, scanInput->config);
}

std::unique_ptr<function::TableFuncSharedState> ParallelCSVScan::initSharedState(
    function::TableFunctionInitInput& input) {
    auto bindData = reinterpret_cast<function::ScanBindData*>(input.bindData);
    common::row_idx_t numRows = 0;
    for (const auto& path : bindData->config.filePaths) {
        auto reader =
            make_unique<SerialCSVReader>(path, bindData->config, bindData->columnNames.size());
        numRows += reader->countRows();
    }
    return std::make_unique<ParallelCSVScanSharedState>(
        bindData->config, numRows, bindData->columnNames.size());
}

std::unique_ptr<function::TableFuncLocalState> ParallelCSVScan::initLocalState(
    function::TableFunctionInitInput& /*input*/, function::TableFuncSharedState* state) {
    auto localState = std::make_unique<ParallelCSVLocalState>();
    auto scanSharedState = reinterpret_cast<ParallelCSVScanSharedState*>(state);
    localState->reader =
        std::make_unique<ParallelCSVReader>(scanSharedState->readerConfig.filePaths[0],
            scanSharedState->readerConfig, scanSharedState->numColumns);
    localState->fileIdx = 0;
    return localState;
}

} // namespace processor
} // namespace kuzu
