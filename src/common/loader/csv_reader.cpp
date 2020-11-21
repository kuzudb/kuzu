#include "src/common/loader/include/csv_reader.h"

#include <fstream>

#include "src/common/include/configs.h"

namespace graphflow {
namespace common {

CSVReader::CSVReader(const string &fname, const char tokenSeparator, uint64_t blockId)
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
        fclose(f);
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
    line[linePtrEnd] = 10;
    return true;
}

unique_ptr<string> CSVReader::getNodeID() {
    return getString();
}

gfInt_t CSVReader::getInteger() {
    int a = atoi(line + linePtrStart);
    nextTokenIsNotProcessed = false;
    return a;
}

gfDouble_t CSVReader::getDouble() {
    double a = atof(line + linePtrStart);
    nextTokenIsNotProcessed = false;
    return a;
}

gfBool_t CSVReader::getBoolean() {
    if (0 == strcmp(line + linePtrStart, trueVal)) {
        nextTokenIsNotProcessed = false;
        return 1;
    }
    if (0 == strcmp(line + linePtrStart, falseVal)) {
        nextTokenIsNotProcessed = false;
        return 2;
    }
    throw invalid_argument("invalid boolean val.");
}

unique_ptr<string> CSVReader::getString() {
    auto a = make_unique<string>(line + linePtrStart, linePtrEnd - linePtrStart);
    nextTokenIsNotProcessed = false;
    return a;
}

} // namespace common
} // namespace graphflow
