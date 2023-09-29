#include "processor/operator/persistent/reader/csv/parallel_csv_reader.h"

#include "common/exception/copy.h"
#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

ParallelCSVReader::ParallelCSVReader(
    const std::string& filePath, const common::ReaderConfig& readerConfig)
    : BaseCSVReader{filePath, readerConfig} {}

bool ParallelCSVReader::hasMoreToRead() const {
    // If we haven't started the first block yet or are done our block, get the next block.
    return buffer != nullptr && !finishedBlockDetail();
}

uint64_t ParallelCSVReader::continueBlock(common::DataChunk& resultChunk) {
    assert(buffer != nullptr && !finishedBlockDetail() && mode == ParserMode::PARSING);
    return parseCSV(resultChunk);
}

void ParallelCSVReader::parseBlockHook() {
    // Seek to the proper location in the file.
    if (lseek(fd, currentBlockIdx * CopyConstants::PARALLEL_BLOCK_SIZE, SEEK_SET) == -1) {
        // LCOV_EXCL_START
        throw CopyException(StringUtils::string_format("Failed to seek to block {} in file {}: {}",
            currentBlockIdx, filePath, strerror(errno)));
        // LCOV_EXCL_END
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
    throw CopyException(
        StringUtils::string_format("Quoted newlines are not supported in parallel CSV reader "
                                   "(while parsing {} on line {}). Please "
                                   "specify PARALLEL=FALSE in the options.",
            filePath, getLineNumber()));
}

bool ParallelCSVReader::finishedBlockDetail() const {
    // Only stop if we've ventured into the next block by at least a byte.
    // Use `>` because `position` points to just past the newline right now.
    return getFileOffset() > (currentBlockIdx + 1) * CopyConstants::PARALLEL_BLOCK_SIZE;
}

} // namespace processor
} // namespace kuzu
