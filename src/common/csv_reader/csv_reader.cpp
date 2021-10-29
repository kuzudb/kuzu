#include "src/common/include/csv_reader/csv_reader.h"

#include "src/common/include/configs.h"
#include "src/common/include/date.h"
#include "src/common/include/interval.h"
#include "src/common/include/timestamp.h"

namespace graphflow {
namespace common {

CSVReader::CSVReader(const string& fName, const char tokenSeparator, const char quoteChar,
    const char escapeChar, uint64_t blockId)
    : fd{nullptr}, tokenSeparator{tokenSeparator}, quoteChar{quoteChar}, escapeChar{escapeChar},
      line{(char*)malloc(sizeof(char) * 1024)}, readingBlockIdx{CSV_READING_BLOCK_SIZE * blockId},
      readingBlockEndIdx{CSV_READING_BLOCK_SIZE * (blockId + 1)} {
    openFile(fName);
    auto isBeginningOfLine = false;
    if (0 == readingBlockIdx) {
        isBeginningOfLine = true;
    } else {
        fseek(fd, readingBlockIdx - 1, SEEK_SET);
        if ('\n' == fgetc(fd)) {
            isBeginningOfLine = true;
        }
    }
    if (!isBeginningOfLine) {
        while ('\n' != fgetc(fd)) {}
    }
}

CSVReader::CSVReader(
    const string& fname, const char tokenSeparator, const char quoteChar, const char escapeChar)
    : tokenSeparator(tokenSeparator), quoteChar{quoteChar},
      escapeChar{escapeChar}, line{(char*)malloc(sizeof(char) * 1024)} {
    openFile(fname);
    readingBlockIdx = 0;
    readingBlockEndIdx = UINT64_MAX;
}

CSVReader::~CSVReader() {
    fclose(fd);
    free(line);
}

bool CSVReader::hasNextLine() {
    // the block has already been ended, return false.
    if (isEndOfBlock) {
        return false;
    }
    // the previous line has not been processed yet, return true.
    if (nextLineIsNotProcessed) {
        return true;
    }
    // file cursor is past the block limit, end the block, return false.
    if (ftell(fd) >= readingBlockEndIdx) {
        isEndOfBlock = true;
        return false;
    }
    // else, read the next line.
    lineLen = getline(&line, &lineCapacity, fd);
    while (2 > lineLen || CSVReader::COMMENT_LINE_CHAR == line[0]) {
        lineLen = getline(&line, &lineCapacity, fd);
    };
    linePtrStart = linePtrEnd = -1;
    // file ends, end the file.
    if (feof(fd)) {
        isEndOfBlock = true;
        return false;
    }
    return true;
}

void CSVReader::skipLine() {
    nextLineIsNotProcessed = false;
}

bool CSVReader::skipTokenIfNull() {
    if (linePtrEnd - linePtrStart == 0) {
        nextLineIsNotProcessed = false;
        return true;
    }
    return false;
}

void CSVReader::skipToken() {
    setNextTokenIsNotProcessed();
}

bool CSVReader::hasNextToken() {
    if (nextTokenIsNotProcessed) {
        return true;
    }
    linePtrEnd++;
    linePtrStart = linePtrEnd;
    if (linePtrEnd == lineLen) {
        nextLineIsNotProcessed = false;
        return false;
    }
    nextTokenLen = 0;
    bool isQuotedString = false;

    if (quoteChar == line[linePtrEnd]) {
        linePtrStart++;
        linePtrEnd++;
        isQuotedString = true;
    }
    string lineStr;
    while (true) {
        if (isQuotedString) {
            // ignore tokenSeparator and new line character here
            if (quoteChar == line[linePtrEnd]) {
                break;
            } else if (escapeChar == line[linePtrEnd]) {
                // escape next special character
                linePtrEnd++;
            }
        } else if (tokenSeparator == line[linePtrEnd] || '\n' == line[linePtrEnd]) {
            break;
        }
        lineStr += line[linePtrEnd];
        nextTokenLen++;
        linePtrEnd++;
    }
    line[linePtrEnd] = 0;
    if (isQuotedString) {
        strncpy(line + linePtrStart, lineStr.c_str(), lineStr.length() + 1);
        // if this is a string literal, skip the next comma as well
        linePtrEnd++;
    }
    return true;
}

int64_t CSVReader::getInt64() {
    setNextTokenIsNotProcessed();
    return TypeUtils::convertToInt64(line + linePtrStart);
}

double_t CSVReader::getDouble() {
    setNextTokenIsNotProcessed();
    return TypeUtils::convertToDouble(line + linePtrStart);
}

uint8_t CSVReader::getBoolean() {
    setNextTokenIsNotProcessed();
    return TypeUtils::convertToBoolean(line + linePtrStart);
}

char* CSVReader::getString() {
    setNextTokenIsNotProcessed();
    return line + linePtrStart;
}

date_t CSVReader::getDate() {
    date_t retVal = Date::FromCString(line + linePtrStart, nextTokenLen);
    setNextTokenIsNotProcessed();
    return retVal;
}

timestamp_t CSVReader::getTimestamp() {
    timestamp_t retVal = Timestamp::FromCString(line + linePtrStart, nextTokenLen);
    setNextTokenIsNotProcessed();
    return retVal;
}

interval_t CSVReader::getInterval() {
    interval_t retVal = Interval::FromCString(line + linePtrStart, nextTokenLen);
    setNextTokenIsNotProcessed();
    return retVal;
}

void CSVReader::setNextTokenIsNotProcessed() {
    nextTokenIsNotProcessed = false;
    nextTokenLen = -1;
}

void CSVReader::openFile(const string& fName) {
    fd = fopen(fName.c_str(), "r");
    if (nullptr == fd) {
        throw invalid_argument("Cannot open file: " + fName);
    }
}

} // namespace common
} // namespace graphflow
