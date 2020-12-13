#include "src/loader/include/csv_reader.h"

#include <fstream>

#include "src/common/include/configs.h"

namespace graphflow {
namespace loader {

CSVReader::CSVReader(const string& fname, const char tokenSeparator, uint64_t blockId)
    : tokenSeparator(tokenSeparator), nextLineIsNotProcessed{false}, isEndOfBlock{false},
      nextTokenIsNotProcessed{false}, line{new char[1 << 10]}, lineCapacity{1 << 10}, lineLen{0},
      linePtrStart{-1l}, linePtrEnd{-1l}, readingBlockIdx(CSV_READING_BLOCK_SIZE * blockId),
      readingBlockEndIdx(CSV_READING_BLOCK_SIZE * (blockId + 1)) {
    f = fopen(fname.c_str(), "r");
    if (nullptr == f) {
        throw invalid_argument("Cannot open file: " + fname);
    }
    auto isBeginningOfLine = false;
    if (0 == readingBlockIdx) {
        isBeginningOfLine = true;
    } else {
        fseek(f, readingBlockIdx - 1, SEEK_SET);
        if ('\n' == fgetc(f)) {
            isBeginningOfLine = true;
        }
    }
    if (!isBeginningOfLine) {
        while ('\n' != fgetc(f)) {}
    }
    nextLineIsNotProcessed = false;
    isEndOfBlock = false;
}

CSVReader::~CSVReader() {
    fclose(f);
    delete[](line);
}

bool CSVReader::hasNextLine() {
    if (isEndOfBlock) {
        return false;
    }
    if (nextLineIsNotProcessed) {
        return true;
    }
    if (ftell(f) >= readingBlockEndIdx) {
        isEndOfBlock = false;
        return false;
    }
    lineLen = getline(&line, &lineCapacity, f);
    while (2 > lineLen || '#' == line[0]) {
        lineLen = getline(&line, &lineCapacity, f);
    };
    linePtrStart = linePtrEnd = -1;
    if (feof(f)) {
        isEndOfBlock = false;
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
    nextTokenIsNotProcessed = false;
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
    while (tokenSeparator != line[linePtrEnd] && '\n' != line[linePtrEnd]) {
        linePtrEnd++;
    }
    line[linePtrEnd] = 0;
    return true;
}

int32_t CSVReader::getInteger() {
    nextTokenIsNotProcessed = false;
    return atoi(line + linePtrStart);
}

double_t CSVReader::getDouble() {
    nextTokenIsNotProcessed = false;
    return atof(line + linePtrStart);
    ;
}

uint8_t CSVReader::getBoolean() {
    nextTokenIsNotProcessed = false;
    if (0 == strcmp(line + linePtrStart, trueVal)) {
        return 1;
    }
    if (0 == strcmp(line + linePtrStart, falseVal)) {
        return 2;
    }
    throw invalid_argument("invalid boolean val.");
}

char* CSVReader::getString() {
    nextTokenIsNotProcessed = false;
    return line + linePtrStart;
}

} // namespace loader
} // namespace graphflow
