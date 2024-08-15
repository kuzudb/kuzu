#include "processor/operator/persistent/reader/csv/base_csv_reader.h"

#include <fcntl.h>

#include <vector>

#include "common/file_system/virtual_file_system.h"
#include "common/string_format.h"
#include "common/system_message.h"
#include "processor/operator/persistent/reader/csv/driver.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

BaseCSVReader::BaseCSVReader(const std::string& filePath, common::CSVOption option,
    uint64_t numColumns, main::ClientContext* context, CSVErrorHandler* errorHandler)
    : option{std::move(option)}, numColumns{numColumns}, currentBlockIdx(0), rowNum(0),
      numErrors(0), buffer{nullptr}, bufferIdx(0), bufferSize{0}, position{0}, lineContext(),
      osFileOffset{0}, errorHandler(errorHandler), rowEmpty{false}, context{context} {
    fileInfo = context->getVFSUnsafe()->openFile(filePath,
        O_RDONLY
#ifdef _WIN32
            | _O_BINARY
#endif
        ,
        context);
}

uint64_t BaseCSVReader::countRows() {
    uint64_t rows = 0;
    handleFirstBlock();

line_start:
    // Pass bufferSize as start to avoid keeping any portion of the buffer.
    if (!maybeReadBuffer(nullptr)) {
        return rows;
    }

    // If the number of columns is 1, every line start indicates a row.
    if (numColumns == 1) {
        rows++;
    }

    if (buffer[position] == '\r') {
        position++;
        goto carriage_return;
    } else if (buffer[position] == '\n') {
        position++;
        goto line_start;
    } else {
        // If we have more than one column, every non-empty line is a row.
        if (numColumns != 1) {
            rows++;
        }
        goto normal;
    }
normal:
    do {
        if (buffer[position] == '\r') {
            position++;
            goto carriage_return;
        } else if (buffer[position] == '\n') {
            position++;
            goto line_start;
        } else if (buffer[position] == option.quoteChar) {
            position++;
            goto in_quotes;
        } else {
            position++;
            // Just a normal character of some kind.
        }
    } while (maybeReadBuffer(nullptr));
    return rows;

carriage_return:
    if (!maybeReadBuffer(nullptr)) {
        return rows;
    }

    if (buffer[position] == '\n') {
        position++;
    }
    goto line_start;

in_quotes:
    if (!maybeReadBuffer(nullptr)) {
        return rows;
    }

    do {
        if (buffer[position] == option.quoteChar) {
            position++;
            goto normal;
        } else if (buffer[position] == option.escapeChar) {
            position++;
            goto escape;
        } else {
            position++;
        }
    } while (maybeReadBuffer(nullptr));
    return rows;

escape:
    if (!maybeReadBuffer(nullptr)) {
        return rows;
    }
    position++;
    goto in_quotes;
}

bool BaseCSVReader::isEOF() const {
    return getFileOffset() >= fileInfo->getFileSize();
}

uint64_t BaseCSVReader::getFileSize() {
    return fileInfo->getFileSize();
}

template<typename Driver>
bool BaseCSVReader::addValue(Driver& driver, uint64_t rowNum, column_id_t columnIdx,
    std::string_view strVal, std::vector<uint64_t>& escapePositions) {
    // insert the line number into the chunk
    if (!escapePositions.empty()) {
        // remove escape characters (if any)
        std::string newVal = "";
        uint64_t prevPos = 0;
        for (auto i = 0u; i < escapePositions.size(); i++) {
            auto nextPos = escapePositions[i];
            newVal += strVal.substr(prevPos, nextPos - prevPos);
            prevPos = nextPos + 1;
        }
        newVal += strVal.substr(prevPos, strVal.size() - prevPos);
        escapePositions.clear();
        return driver.addValue(rowNum, columnIdx, newVal);
    } else {
        return driver.addValue(rowNum, columnIdx, strVal);
    }
}

struct SkipRowDriver {
    explicit SkipRowDriver(uint64_t skipNum) : skipNum{skipNum} {}
    bool done(uint64_t rowNum) const { return rowNum >= skipNum; }
    bool addRow(uint64_t, column_id_t) { return true; }
    bool addValue(uint64_t, column_id_t, std::string_view) { return true; }

    uint64_t skipNum;
};

uint64_t BaseCSVReader::handleFirstBlock() {
    uint64_t numRowsRead = 0;
    readBOM();
    if (option.skipNum > 0) {
        SkipRowDriver driver{option.skipNum};
        numRowsRead += parseCSV(driver);
    }
    if (option.hasHeader) {
        numRowsRead += readHeader();
    }
    return numRowsRead;
}

void BaseCSVReader::readBOM() {
    if (!maybeReadBuffer(nullptr)) {
        return;
    }
    if (bufferSize >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
        position = 3;
    }
}

// Dummy driver that just skips a row.
struct HeaderDriver {
    bool done(uint64_t) { return true; }
    bool addRow(uint64_t, column_id_t) { return true; }
    bool addValue(uint64_t, column_id_t, std::string_view) { return true; }
};

uint64_t BaseCSVReader::readHeader() {
    HeaderDriver driver;
    return parseCSV(driver);
}

bool BaseCSVReader::readBuffer(uint64_t* start) {
    std::unique_ptr<char[]> oldBuffer = std::move(buffer);

    // the remaining part of the last buffer
    uint64_t remaining = 0;
    if (start != nullptr) {
        KU_ASSERT(*start <= bufferSize);
        remaining = bufferSize - *start;
    }

    uint64_t bufferReadSize = CopyConstants::INITIAL_BUFFER_SIZE;
    while (remaining > bufferReadSize) {
        bufferReadSize *= 2;
    }

    buffer = std::unique_ptr<char[]>(new char[bufferReadSize + remaining + 1]());
    bufferSize = remaining + bufferReadSize;
    if (remaining > 0) {
        // remaining from last buffer: copy it here
        KU_ASSERT(start != nullptr);
        memcpy(buffer.get(), oldBuffer.get() + *start, remaining);
    }
    auto readCount = fileInfo->readFile(buffer.get() + remaining, bufferReadSize);
    if (readCount == -1) {
        // LCOV_EXCL_START
        handleCopyException(stringFormat("Could not read from file: {}", posixErrMessage()), true);
        // LCOV_EXCL_STOP
    }
    osFileOffset += readCount;

    bufferSize = remaining + readCount;
    buffer[bufferSize] = '\0';
    if (start != nullptr) {
        *start = 0;
    }
    position = remaining;
    ++bufferIdx;
    return readCount > 0;
}

std::string BaseCSVReader::reconstructLine(uint64_t startPosition, uint64_t endPosition) {
    KU_ASSERT(endPosition >= startPosition);

    if (-1 == fileInfo->seek(startPosition, SEEK_SET)) {
        return "";
    }

    std::string res;
    res.resize(endPosition - startPosition);

    (void)fileInfo->readFile(res.data(), res.size());

    [[maybe_unused]] const auto reseekSuccess = fileInfo->seek(osFileOffset, SEEK_SET);
    KU_ASSERT(-1 != reseekSuccess);

    return res;
}

void BaseCSVReader::skipUntilNextLine() {
    do {
        for (; position < bufferSize; ++position) {
            if (isNewLine(buffer[position])) {
                ++position;
                return;
            }
        }
    } while (maybeReadBuffer(nullptr));
    return;
}

template<typename Driver>
uint64_t BaseCSVReader::skipRowAndRestartParse(Driver& driver) {
    skipUntilNextLine();
    return parseCSV(driver, false);
}

void BaseCSVReader::handleCopyException(const std::string& message, bool mustThrow) {
    CSVError error{.message = message,
        .filePath = fileInfo->path,
        .errorLine = lineContext,
        .blockIdx = currentBlockIdx,
        .numRowsReadInBlock = getNumRowsReadInBlock() + rowNum + numErrors};
    if (!error.errorLine.isCompleteLine) {
        error.errorLine.endByteOffset = getFileOffset();
    }
    errorHandler->handleError(this, error, mustThrow);

    // if we reach here it means we are ignoring the error
    ++numErrors;
}

template<typename Driver>
uint64_t BaseCSVReader::parseCSV(Driver& driver, bool resetRowNum) {
    KU_ASSERT(nullptr != errorHandler);

    // used for parsing algorithm
    if (resetRowNum) {
        rowNum = 0;
        numErrors = 0;
    }
    column_id_t column = 0;
    uint64_t start = position;
    bool hasQuotes = false;
    std::vector<uint64_t> escapePositions;
    lineContext.setNewLine(getFileOffset());

    // read values into the buffer (if any)
    if (!maybeReadBuffer(&start)) {
        return rowNum;
    }

    // start parsing the first value
    goto value_start;
value_start:
    /* state: value_start */
    // this state parses the first character of a value
    if (buffer[position] == option.quoteChar) {
        [[unlikely]]
        // quote: actual value starts in the next position
        // move to in_quotes state
        start = position + 1;
        hasQuotes = true;
        goto in_quotes;
    } else {
        // no quote, move to normal parsing state
        start = position;
        hasQuotes = false;
        goto normal;
    }
normal:
    /* state: normal parsing state */
    // this state parses the remainder of a non-quoted value until we reach a delimiter or
    // newline
    do {
        for (; position < bufferSize; position++) {
            if (buffer[position] == option.delimiter) {
                // delimiter: end the value and add it to the chunk
                goto add_value;
            } else if (isNewLine(buffer[position])) {
                // newline: add row
                goto add_row;
            }
        }
    } while (readBuffer(&start));

    [[unlikely]]
    // file ends during normal scan: go to end state
    goto final_state;
add_value:
    // We get here after we have a delimiter.
    KU_ASSERT(buffer[position] == option.delimiter);
    // Trim one character if we have quotes.
    if (!addValue(driver, rowNum, column,
            std::string_view(buffer.get() + start, position - start - hasQuotes),
            escapePositions)) {
        return skipRowAndRestartParse(driver);
    }
    column++;

    // Move past the delimiter.
    ++position;
    // Adjust start for MaybeReadBuffer.
    start = position;
    if (!maybeReadBuffer(&start)) {
        [[unlikely]]
        // File ends right after delimiter, go to final state
        goto final_state;
    }
    goto value_start;
add_row: {
    // We get here after we have a newline.
    KU_ASSERT(isNewLine(buffer[position]));
    lineContext.setEndOfLine(getFileOffset());
    bool isCarriageReturn = buffer[position] == '\r';
    if (!addValue(driver, rowNum, column,
            std::string_view(buffer.get() + start, position - start - hasQuotes),
            escapePositions)) {
        return skipRowAndRestartParse(driver);
    }
    column++;

    // if we are ignoring errors and there is one in the current row skip it
    rowNum += driver.addRow(rowNum, column);

    column = 0;
    position++;
    // Adjust start for ReadBuffer.
    start = position;
    lineContext.setNewLine(getFileOffset());
    if (!maybeReadBuffer(&start)) {
        // File ends right after newline, go to final state.
        goto final_state;
    }
    if (isCarriageReturn) {
        // \r newline, go to special state that parses an optional \n afterwards
        goto carriage_return;
    } else {
        if (driver.done(rowNum)) {
            return rowNum;
        }
        goto value_start;
    }
}
in_quotes:
    // this state parses the remainder of a quoted value.
    position++;
    do {
        for (; position < bufferSize; position++) {
            if (buffer[position] == option.quoteChar) {
                // quote: move to unquoted state
                goto unquote;
            } else if (buffer[position] == option.escapeChar) {
                // escape: store the escaped position and move to handle_escape state
                escapePositions.push_back(position - start);
                goto handle_escape;
            } else if (isNewLine(buffer[position])) {
                [[unlikely]] if (!handleQuotedNewline()) { return skipRowAndRestartParse(driver); }
            }
        }
    } while (readBuffer(&start));
    [[unlikely]]
    // still in quoted state at the end of the file, error:
    lineContext.setEndOfLine(getFileOffset());
    handleCopyException("unterminated quotes.");
    // we are ignoring this error, skip current row and restart state machine
    return skipRowAndRestartParse(driver);
unquote:
    KU_ASSERT(hasQuotes && buffer[position] == option.quoteChar);
    // this state handles the state directly after we unquote
    // in this state we expect either another quote (entering the quoted state again, and
    // escaping the quote) or a delimiter/newline, ending the current value and moving on to the
    // next value
    position++;
    if (!maybeReadBuffer(&start)) {
        // file ends right after unquote, go to final state
        goto final_state;
    }
    if (buffer[position] == option.quoteChar &&
        (!option.escapeChar || option.escapeChar == option.quoteChar)) {
        // escaped quote, return to quoted state and store escape position
        escapePositions.push_back(position - start);
        goto in_quotes;
    } else if (buffer[position] == option.delimiter ||
               buffer[position] == CopyConstants::DEFAULT_CSV_LIST_END_CHAR) {
        // delimiter, add value
        goto add_value;
    } else if (isNewLine(buffer[position])) {
        goto add_row;
    } else {
        [[unlikely]] handleCopyException("quote should be followed by "
                                         "end of file, end of value, end of "
                                         "row or another quote.");
        return skipRowAndRestartParse(driver);
    }
handle_escape:
    /* state: handle_escape */
    // escape should be followed by a quote or another escape character
    position++;
    if (!maybeReadBuffer(&start)) {
        [[unlikely]] lineContext.setEndOfLine(getFileOffset());
        handleCopyException("escape at end of file.");
        return skipRowAndRestartParse(driver);
    }
    if (buffer[position] != option.quoteChar && buffer[position] != option.escapeChar) {
        ++position; // consume the invalid char
        [[unlikely]] handleCopyException("neither QUOTE nor ESCAPE is proceeded by ESCAPE.");
        return skipRowAndRestartParse(driver);
    }
    // escape was followed by quote or escape, go back to quoted state
    goto in_quotes;
carriage_return:
    // this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted
    // as a single line

    // position points to the character after the carriage return.
    if (buffer[position] == '\n') {
        // newline after carriage return: skip
        // increase position by 1 and move start to the new position
        start = ++position;
        if (!maybeReadBuffer(&start)) {
            // file ends right after newline, go to final state
            goto final_state;
        }
    }
    if (driver.done(rowNum)) {
        return rowNum;
    }

    goto value_start;
final_state:
    // We get here when the file ends.
    // If we were mid-value, add the remaining value to the chunk.
    lineContext.setEndOfLine(getFileOffset());
    if (position > start) {
        // Add remaining value to chunk.
        if (!addValue(driver, rowNum, column,
                std::string_view(buffer.get() + start, position - start - hasQuotes),
                escapePositions)) {
            return skipRowAndRestartParse(driver);
        }
        column++;
    }
    if (column > 0) {
        rowNum += driver.addRow(rowNum, column);
    }
    return rowNum;
}

template uint64_t BaseCSVReader::parseCSV<ParallelParsingDriver>(ParallelParsingDriver&, bool);
template uint64_t BaseCSVReader::parseCSV<SerialParsingDriver>(SerialParsingDriver&, bool);
template uint64_t BaseCSVReader::parseCSV<SniffCSVNameAndTypeDriver>(SniffCSVNameAndTypeDriver&,
    bool);

uint64_t BaseCSVReader::getFileOffset() const {
    KU_ASSERT(osFileOffset >= bufferSize);
    return osFileOffset - bufferSize + position;
}

} // namespace processor
} // namespace kuzu
