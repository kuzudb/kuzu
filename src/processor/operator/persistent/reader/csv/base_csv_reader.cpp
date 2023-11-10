#include "processor/operator/persistent/reader/csv/base_csv_reader.h"

#include <fcntl.h>
#if defined(_WIN32)
#include <io.h>
#else
#include <unistd.h>
#endif

#include <vector>

#include "common/exception/copy.h"
#include "common/string_format.h"
#include "common/system_message.h"
#include "processor/operator/persistent/reader/csv/driver.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

BaseCSVReader::BaseCSVReader(
    const std::string& filePath, const common::ReaderConfig& readerConfig, uint64_t numColumns)
    : filePath{filePath}, csvReaderConfig{*readerConfig.csvReaderConfig}, numColumns(numColumns),
      fd(-1), buffer(nullptr), bufferSize(0), position(0), rowEmpty(false) {
    // TODO(Ziyi): should we wrap this fd using kuzu file handler?
    fd = open(filePath.c_str(), O_RDONLY
#ifdef _WIN32
                                    | _O_BINARY
#endif
    );
    if (fd == -1) {
        // LCOV_EXCL_START
        throw CopyException(
            stringFormat("Could not open file {}: {}", filePath, posixErrMessage()));
        // LCOV_EXCL_STOP
    }
}

BaseCSVReader::~BaseCSVReader() {
    if (fd != -1) {
        close(fd);
    }
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
        } else if (buffer[position] == csvReaderConfig.quoteChar) {
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
        if (buffer[position] == csvReaderConfig.quoteChar) {
            position++;
            goto normal;
        } else if (buffer[position] == csvReaderConfig.escapeChar) {
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
    uint64_t offset = getFileOffset();
    uint64_t end = lseek(fd, 0, SEEK_END);
    if (end == -1) {
        // LCOV_EXCL_START
        throw CopyException(
            stringFormat("Could not seek to end of file {}: {}", filePath, posixErrMessage()));
        // LCOV_EXCL_STOP
    }
    if (lseek(fd, offset, SEEK_SET) == -1) {
        // LCOV_EXCL_START
        throw CopyException(
            stringFormat("Could not reset position of file {}: {}", filePath, posixErrMessage()));
        // LCOV_EXCL_STOP
    }
    return offset >= end;
}

template<typename Driver>
void BaseCSVReader::addValue(Driver& driver, uint64_t rowNum, column_id_t columnIdx,
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
        driver.addValue(rowNum, columnIdx, newVal);
    } else {
        driver.addValue(rowNum, columnIdx, strVal);
    }
}

void BaseCSVReader::handleFirstBlock() {
    readBOM();
    if (csvReaderConfig.hasHeader) {
        readHeader();
    }
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
    void addValue(uint64_t, column_id_t, std::string_view) {}
};

void BaseCSVReader::readHeader() {
    HeaderDriver driver;
    parseCSV(driver);
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

    uint64_t readCount = read(fd, buffer.get() + remaining, bufferReadSize);
    if (readCount == -1) {
        // LCOV_EXCL_START
        throw CopyException(
            stringFormat("Could not read from file {}: {}", filePath, posixErrMessage()));
        // LCOV_EXCL_STOP
    }

    bufferSize = remaining + readCount;
    buffer[bufferSize] = '\0';
    if (start != nullptr) {
        *start = 0;
    }
    position = remaining;
    return readCount > 0;
}

template<typename Driver>
uint64_t BaseCSVReader::parseCSV(Driver& driver) {
    // used for parsing algorithm
    uint64_t rowNum = 0;
    column_id_t column = 0;
    uint64_t start = position;
    bool hasQuotes = false;
    std::vector<uint64_t> escapePositions;

    // read values into the buffer (if any)
    if (!maybeReadBuffer(&start)) {
        return 0;
    }

    // start parsing the first value
    goto value_start;
value_start:
    /* state: value_start */
    // this state parses the first character of a value
    if (buffer[position] == csvReaderConfig.quoteChar) {
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
            if (buffer[position] == csvReaderConfig.delimiter) {
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
    KU_ASSERT(buffer[position] == csvReaderConfig.delimiter);
    // Trim one character if we have quotes.
    addValue(driver, rowNum, column,
        std::string_view(buffer.get() + start, position - start - hasQuotes), escapePositions);
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
add_row : {
    // We get here after we have a newline.
    KU_ASSERT(isNewLine(buffer[position]));
    bool isCarriageReturn = buffer[position] == '\r';
    addValue(driver, rowNum, column,
        std::string_view(buffer.get() + start, position - start - hasQuotes), escapePositions);
    column++;

    rowNum += driver.addRow(rowNum, column);

    column = 0;
    position++;
    // Adjust start for ReadBuffer.
    start = position;
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
            if (buffer[position] == csvReaderConfig.quoteChar) {
                // quote: move to unquoted state
                goto unquote;
            } else if (buffer[position] == csvReaderConfig.escapeChar) {
                // escape: store the escaped position and move to handle_escape state
                escapePositions.push_back(position - start);
                goto handle_escape;
            } else if (isNewLine(buffer[position])) {
                [[unlikely]] handleQuotedNewline();
            }
        }
    } while (readBuffer(&start));
    [[unlikely]]
    // still in quoted state at the end of the file, error:
    throw CopyException(stringFormat(
        "Error in file {} on line {}: unterminated quotes.", filePath, getLineNumber()));
unquote:
    KU_ASSERT(hasQuotes && buffer[position] == csvReaderConfig.quoteChar);
    // this state handles the state directly after we unquote
    // in this state we expect either another quote (entering the quoted state again, and
    // escaping the quote) or a delimiter/newline, ending the current value and moving on to the
    // next value
    position++;
    if (!maybeReadBuffer(&start)) {
        // file ends right after unquote, go to final state
        goto final_state;
    }
    if (buffer[position] == csvReaderConfig.quoteChar &&
        (!csvReaderConfig.escapeChar || csvReaderConfig.escapeChar == csvReaderConfig.quoteChar)) {
        // escaped quote, return to quoted state and store escape position
        escapePositions.push_back(position - start);
        goto in_quotes;
    } else if (buffer[position] == csvReaderConfig.delimiter ||
               buffer[position] == CopyConstants::DEFAULT_CSV_LIST_END_CHAR) {
        // delimiter, add value
        goto add_value;
    } else if (isNewLine(buffer[position])) {
        goto add_row;
    } else {
        [[unlikely]] throw CopyException(
            stringFormat("Error in file {} on line {}: quote should be followed by "
                         "end of file, end of value, end of "
                         "row or another quote.",
                filePath, getLineNumber()));
    }
handle_escape:
    /* state: handle_escape */
    // escape should be followed by a quote or another escape character
    position++;
    if (!maybeReadBuffer(&start)) {
        [[unlikely]] throw CopyException(stringFormat(
            "Error in file {} on line {}: escape at end of file.", filePath, getLineNumber()));
    }
    if (buffer[position] != csvReaderConfig.quoteChar &&
        buffer[position] != csvReaderConfig.escapeChar) {
        [[unlikely]] throw CopyException(stringFormat(
            "Error in file {} on line {}: neither QUOTE nor ESCAPE is proceeded by ESCAPE.",
            filePath, getLineNumber()));
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
    if (position > start) {
        // Add remaining value to chunk.
        addValue(driver, rowNum, column,
            std::string_view(buffer.get() + start, position - start - hasQuotes), escapePositions);
        column++;
    }
    if (column > 0) {
        rowNum += driver.addRow(rowNum, column);
    }
    return rowNum;
}

template uint64_t BaseCSVReader::parseCSV<ParallelParsingDriver>(ParallelParsingDriver&);
template uint64_t BaseCSVReader::parseCSV<SerialParsingDriver>(SerialParsingDriver&);
template uint64_t BaseCSVReader::parseCSV<SniffCSVNameAndTypeDriver>(SniffCSVNameAndTypeDriver&);
template uint64_t BaseCSVReader::parseCSV<SniffCSVColumnCountDriver>(SniffCSVColumnCountDriver&);

uint64_t BaseCSVReader::getFileOffset() const {
    uint64_t offset = lseek(fd, 0, SEEK_CUR);
    if (offset == -1) {
        // LCOV_EXCL_START
        throw CopyException(stringFormat(
            "Could not get current file position for file {}: {}", filePath, posixErrMessage()));
        // LCOV_EXCL_STOP
    }
    KU_ASSERT(offset >= bufferSize);
    return offset - bufferSize + position;
}

uint64_t BaseCSVReader::getLineNumber() {
    uint64_t offset = getFileOffset();
    uint64_t lineNumber = 1;
    const uint64_t BUF_SIZE = 4096;
    char buf[BUF_SIZE];
    if (lseek(fd, 0, SEEK_SET) == -1) {
        // LCOV_EXCL_START
        throw CopyException(stringFormat(
            "Could not seek to beginning of file {}: {}", filePath, posixErrMessage()));
        // LCOV_EXCL_STOP
    }

    bool carriageReturn = false;
    uint64_t totalBytes = 0;
    do {
        uint64_t bytesRead = read(fd, buf, std::min(BUF_SIZE, offset - totalBytes));
        if (bytesRead == -1) {
            // LCOV_EXCL_START
            throw CopyException(
                stringFormat("Could not read from file {}: {}", filePath, posixErrMessage()));
            // LCOV_EXCL_STOP
        }
        totalBytes += bytesRead;

        for (uint64_t i = 0; i < bytesRead; i++) {
            if (buf[i] == '\n' || carriageReturn) {
                lineNumber++;
                carriageReturn = false;
            }
            if (buf[i] == '\r') {
                carriageReturn = true;
            }
        }
    } while (totalBytes < offset);
    return lineNumber + carriageReturn;
}

} // namespace processor
} // namespace kuzu
