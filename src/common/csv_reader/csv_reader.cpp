#include "src/common/include/csv_reader/csv_reader.h"

#include "src/common/include/configs.h"
#include "src/common/include/type_utils.h"
#include "src/common/include/utils.h"

namespace graphflow {
namespace common {

CSVReader::CSVReader(const string& fName, const CSVReaderConfig& config, uint64_t blockId)
    : CSVReader{fName, config} {
    readingBlockStartOffset = LoaderConfig::CSV_READING_BLOCK_SIZE * blockId;
    readingBlockEndOffset = LoaderConfig::CSV_READING_BLOCK_SIZE * (blockId + 1);
    auto isBeginningOfLine = false;
    if (0 == readingBlockStartOffset) {
        isBeginningOfLine = true;
    } else {
        fseek(fd, readingBlockStartOffset - 1, SEEK_SET);
        if ('\n' == fgetc(fd)) {
            isBeginningOfLine = true;
        }
    }
    if (!isBeginningOfLine) {
        while ('\n' != fgetc(fd)) {}
    }
}

CSVReader::CSVReader(const string& fname, const CSVReaderConfig& config)
    : CSVReader{(char*)malloc(sizeof(char) * 1024), 0, -1l, config} {
    openFile(fname);
}

CSVReader::CSVReader(
    char* line, uint64_t lineLen, int64_t linePtrStart, const CSVReaderConfig& config)
    : fd{nullptr}, config{config}, nextLineIsNotProcessed{false}, isEndOfBlock{false},
      nextTokenIsNotProcessed{false}, line{line}, lineCapacity{1024}, lineLen{lineLen},
      linePtrStart{linePtrStart}, linePtrEnd{linePtrStart}, readingBlockStartOffset{0},
      readingBlockEndOffset{UINT64_MAX}, nextTokenLen{UINT64_MAX} {}

CSVReader::~CSVReader() {
    // fd can be nullptr when the CSVReader is constructed by passing a char*, so it is reading over
    // a substring instead of a file.
    if (fd != nullptr) {
        fclose(fd);
        free(line);
    }
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
    if (ftell(fd) >= readingBlockEndOffset) {
        isEndOfBlock = true;
        return false;
    }
    // else, read the next line. The function getline() will dynamically allocate a larger line and
    // update the lineCapacity accordingly if the length of the line exceeds the lineCapacity.
    lineLen = getline(&line, &lineCapacity, fd);
    while (2 > lineLen || LoaderConfig::COMMENT_LINE_CHAR == line[0]) {
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
    setNextTokenIsProcessed();
}

bool CSVReader::hasNextToken() {
    if (nextTokenIsNotProcessed) {
        return true;
    }
    linePtrEnd++;
    linePtrStart = linePtrEnd;
    if (linePtrEnd >= lineLen) {
        nextLineIsNotProcessed = false;
        return false;
    }
    nextTokenLen = 0;
    bool isQuotedString = false;
    uint32_t nestedListLevel = 0;
    bool isList = false;

    if (config.quoteChar == line[linePtrEnd]) {
        linePtrStart++;
        linePtrEnd++;
        isQuotedString = true;
    }
    if (config.listBeginChar == line[linePtrEnd]) {
        linePtrStart++;
        linePtrEnd++;
        nestedListLevel++;
        isList = true;
    }
    string lineStr;
    while (true) {
        if (isQuotedString) {
            // ignore tokenSeparator and new line character here
            if (config.quoteChar == line[linePtrEnd]) {
                break;
            } else if (config.escapeChar == line[linePtrEnd]) {
                // escape next special character
                linePtrEnd++;
            }
        } else if (isList) {
            // ignore tokenSeparator and new line character here
            if (config.listBeginChar == line[linePtrEnd]) {
                linePtrEnd++;
                nestedListLevel++;
            } else if (config.listEndChar == line[linePtrEnd]) {
                nestedListLevel--;
            }
            if (nestedListLevel == 0) {
                break;
            }
        } else if (config.tokenSeparator == line[linePtrEnd] || '\n' == line[linePtrEnd] ||
                   linePtrEnd == lineLen) {
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
    if (isList) {
        // skip the next comma
        linePtrEnd++;
    }
    return true;
}

int64_t CSVReader::getInt64() {
    setNextTokenIsProcessed();
    return TypeUtils::convertToInt64(line + linePtrStart);
}

double_t CSVReader::getDouble() {
    setNextTokenIsProcessed();
    return TypeUtils::convertToDouble(line + linePtrStart);
}

uint8_t CSVReader::getBoolean() {
    setNextTokenIsProcessed();
    return TypeUtils::convertToBoolean(line + linePtrStart);
}

char* CSVReader::getString() {
    setNextTokenIsProcessed();
    auto strVal = line + linePtrStart;
    if (strlen(strVal) > DEFAULT_PAGE_SIZE) {
        throw CSVReaderException(StringUtils::getLongStringErrorMessage(strVal, DEFAULT_PAGE_SIZE));
    }
    return strVal;
}

date_t CSVReader::getDate() {
    date_t retVal = Date::FromCString(line + linePtrStart, nextTokenLen);
    setNextTokenIsProcessed();
    return retVal;
}

timestamp_t CSVReader::getTimestamp() {
    timestamp_t retVal = Timestamp::FromCString(line + linePtrStart, nextTokenLen);
    setNextTokenIsProcessed();
    return retVal;
}

interval_t CSVReader::getInterval() {
    interval_t retVal = Interval::FromCString(line + linePtrStart, nextTokenLen);
    setNextTokenIsProcessed();
    return retVal;
}

Literal CSVReader::getList(const DataType& dataType) {
    Literal result(DataType(LIST, make_unique<DataType>(dataType)));
    // Move the linePtrStart one character forward, because hasNextToken() will first increment it.
    CSVReader listCSVReader(line, linePtrEnd - 1, linePtrStart - 1, config);
    while (listCSVReader.hasNextToken()) {
        if (!listCSVReader.skipTokenIfNull()) {
            switch (dataType.typeID) {
            case INT64: {
                result.listVal.emplace_back(listCSVReader.getInt64());
            } break;
            case DOUBLE: {
                result.listVal.emplace_back(listCSVReader.getDouble());
            } break;
            case BOOL: {
                result.listVal.emplace_back((bool)listCSVReader.getBoolean());
            } break;
            case STRING: {
                result.listVal.emplace_back(string(listCSVReader.getString()));
            } break;
            case DATE: {
                result.listVal.emplace_back(listCSVReader.getDate());
            } break;
            case TIMESTAMP: {
                result.listVal.emplace_back(listCSVReader.getTimestamp());
            } break;
            case INTERVAL: {
                result.listVal.emplace_back(listCSVReader.getInterval());
            } break;
            case LIST: {
                result.listVal.emplace_back(listCSVReader.getList(*dataType.childType));
            } break;
            default:
                throw CSVReaderException("Unsupported data type " +
                                         Types::dataTypeToString(dataType.childType->typeID) +
                                         " inside LIST");
            }
        }
    }
    auto numBytesOfOverflow = result.listVal.size() * Types::getDataTypeSize(dataType.typeID);
    if (numBytesOfOverflow >= DEFAULT_PAGE_SIZE) {
        throw CSVReaderException(StringUtils::string_format(
            "Maximum num bytes of a LIST is %d. Input list's num bytes is %d.", DEFAULT_PAGE_SIZE,
            numBytesOfOverflow));
    }
    return result;
}

void CSVReader::setNextTokenIsProcessed() {
    nextTokenIsNotProcessed = false;
    nextTokenLen = UINT64_MAX;
}

void CSVReader::openFile(const string& fName) {
    fd = fopen(fName.c_str(), "r");
    if (nullptr == fd) {
        throw CSVReaderException("Cannot open file: " + fName);
    }
}

} // namespace common
} // namespace graphflow
