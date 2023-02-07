#include "common/csv_reader/csv_reader.h"

#include "common/configs.h"
#include "common/type_utils.h"
#include "common/utils.h"
#include "spdlog/spdlog.h"
#include "utf8proc_wrapper.h"

using namespace kuzu::utf8proc;

namespace kuzu {
namespace common {

CSVReader::CSVReader(const std::string& fName, const CSVReaderConfig& config, uint64_t blockId)
    : CSVReader{fName, config} {
    readingBlockStartOffset = CopyConfig::CSV_READING_BLOCK_SIZE * blockId;
    readingBlockEndOffset = CopyConfig::CSV_READING_BLOCK_SIZE * (blockId + 1);
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

CSVReader::CSVReader(const std::string& fname, const CSVReaderConfig& config)
    : CSVReader{(char*)malloc(sizeof(char) * 1024), 0, -1l, config} {
    openFile(fname);
}

CSVReader::CSVReader(
    char* line, uint64_t lineLen, int64_t linePtrStart, const CSVReaderConfig& config)
    : fd{nullptr}, config{config}, logger{LoggerUtils::getOrCreateLogger("csv_reader")},
      nextLineIsNotProcessed{false}, isEndOfBlock{false},
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
    auto curPos = ftell(fd);
    if (curPos >= readingBlockEndOffset) {
        isEndOfBlock = true;
        return false;
    }
    // else, read the next line. The function getline() will dynamically allocate a larger line and
    // update the lineCapacity accordingly if the length of the line exceeds the lineCapacity.
    // We keep curPos in case the very final line does not have a \n character in which case
    // we will seek back to where we were and read it without using getLine (inside the if).
    lineLen = getline(&line, &lineCapacity, fd);
    if (lineLen == (ssize_t)-1) {
        isEndOfBlock = true;
        return false;
    }
    // Text files created on DOS/Windows machines have different line endings than files created on
    // Unix/Linux. DOS uses carriage return and line feed ("\r\n") as a line ending, which Unix uses
    // just line feed ("\n"). If the current line uses dos-style newline, we should replace the
    // '\r\n' with the linux-style newline '\n'.
    if (lineLen > 1 && line[lineLen - 1] == '\n' && line[lineLen - 2] == '\r') {
        line[lineLen - 2] = '\n';
        lineLen -= 1;
    }
    if (feof(fd)) {
        // According to POSIX getline manual (https://man7.org/linux/man-pages/man3/getline.3.html)
        // the behavior of getline when in reaches an end of file is underdefined in terms of how
        // it leaves the first (line) argument above. Instead, we re-read the file, this time
        // with fgets (https://en.cppreference.com/w/c/io/fgets), whose behavior is clear and will
        // guarantee.
        // We first determine the last offset of the file:
        fseek(fd, 0L, SEEK_END);
        auto lastPos = ftell(fd);
        isEndOfBlock = true;
        auto sizeOfRemainder = lastPos - curPos;
        if (sizeOfRemainder <= 0) {
            return false;
        }

        if (lineCapacity < sizeOfRemainder) {
            // Note: We don't have tests testing this case because although according to
            // getline's documentation, the behavior is undefined, the getline call above
            // (before the feof check) seems to be increasing the lineCapacity for the
            // last lines without newline character. So this is here for safety but is
            // not tested.
            free(line);
            // We are adding + 1 for the additional \n character we will append.
            line = (char*)malloc(sizeOfRemainder + 1);
        }
        fseek(fd, curPos, SEEK_SET);
        fgets(line, (int)sizeOfRemainder + 1, fd);
        line[sizeOfRemainder] = '\n';
        lineLen = sizeOfRemainder;
    }
    // The line is empty
    if (lineLen < 2) {
        return false;
    }
    linePtrStart = linePtrEnd = -1;
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
    std::string lineStr;
    while (true) {
        if (isQuotedString) {
            // ignore delimiter and new line character here
            if (config.quoteChar == line[linePtrEnd]) {
                break;
            } else if (config.escapeChar == line[linePtrEnd]) {
                // escape next special character
                linePtrEnd++;
            }
        } else if (isList) {
            // ignore delimiter and new line character here
            if (config.listBeginChar == line[linePtrEnd]) {
                linePtrEnd++;
                nestedListLevel++;
            } else if (config.listEndChar == line[linePtrEnd]) {
                nestedListLevel--;
            }
            if (nestedListLevel == 0) {
                break;
            }
        } else if (config.delimiter == line[linePtrEnd] || '\n' == line[linePtrEnd] ||
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
        // if this is a std::string literal, skip the next comma as well
        linePtrEnd++;
    }
    if (isList) {
        // skip the next comma
        linePtrEnd++;
    }
    return true;
}

bool CSVReader::hasNextTokenOrError() {
    if (!hasNextToken()) {
        throw ReaderException(
            StringUtils::string_format("CSV Reader was expecting more tokens but the line does not "
                                       "have any tokens left. Last token: %s",
                line + linePtrStart));
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
        logger->warn(StringUtils::getLongStringErrorMessage(strVal, DEFAULT_PAGE_SIZE));
        // If the std::string is too long, truncate it.
        strVal[DEFAULT_PAGE_SIZE] = '\0';
    }
    auto unicodeType = Utf8Proc::analyze(strVal, strlen(strVal));
    if (unicodeType == UnicodeType::ASCII) {
        return strVal;
    } else if (unicodeType == UnicodeType::UNICODE) {
        return Utf8Proc::normalize(strVal, strlen(strVal));
    } else {
        throw ReaderException("Invalid UTF-8 character encountered.");
    }
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

std::unique_ptr<Value> CSVReader::getList(const DataType& dataType) {
    std::vector<std::unique_ptr<Value>> listVal;
    // Move the linePtrStart one character forward, because hasNextToken() will first increment it.
    CSVReader listCSVReader(line, linePtrEnd - 1, linePtrStart - 1, config);
    while (listCSVReader.hasNextToken()) {
        if (!listCSVReader.skipTokenIfNull()) {
            std::unique_ptr<Value> val;
            switch (dataType.typeID) {
            case INT64: {
                val = std::make_unique<Value>(listCSVReader.getInt64());
            } break;
            case DOUBLE: {
                val = std::make_unique<Value>(listCSVReader.getDouble());
            } break;
            case BOOL: {
                val = std::make_unique<Value>((bool)listCSVReader.getBoolean());
            } break;
            case STRING: {
                val = std::make_unique<Value>(std::string(listCSVReader.getString()));
            } break;
            case DATE: {
                val = std::make_unique<Value>(listCSVReader.getDate());
            } break;
            case TIMESTAMP: {
                val = std::make_unique<Value>(listCSVReader.getTimestamp());
            } break;
            case INTERVAL: {
                val = std::make_unique<Value>(listCSVReader.getInterval());
            } break;
            case LIST: {
                val = listCSVReader.getList(*dataType.childType);
            } break;
            default:
                throw ReaderException("Unsupported data type " +
                                      Types::dataTypeToString(dataType.childType->typeID) +
                                      " inside LIST");
            }
            listVal.push_back(std::move(val));
        }
    }
    auto numBytesOfOverflow = listVal.size() * Types::getDataTypeSize(dataType.typeID);
    if (numBytesOfOverflow >= DEFAULT_PAGE_SIZE) {
        throw ReaderException(StringUtils::string_format(
            "Maximum num bytes of a LIST is %d. Input list's num bytes is %d.", DEFAULT_PAGE_SIZE,
            numBytesOfOverflow));
    }
    return std::make_unique<Value>(
        DataType(LIST, std::make_unique<DataType>(dataType)), std::move(listVal));
}

void CSVReader::setNextTokenIsProcessed() {
    nextTokenIsNotProcessed = false;
    nextTokenLen = UINT64_MAX;
}

void CSVReader::openFile(const std::string& fName) {
    fd = fopen(fName.c_str(), "r");
    if (nullptr == fd) {
        throw ReaderException("Cannot open file: " + fName);
    }
}

CopyDescription::CopyDescription(const std::string& filePath, CSVReaderConfig csvReaderConfig)
    : filePath{filePath}, csvReaderConfig{nullptr}, fileType{FileType::CSV} {
    setFileType(filePath);
    if (fileType == FileType::CSV) {
        this->csvReaderConfig = std::make_unique<CSVReaderConfig>(csvReaderConfig);
    }
}

CopyDescription::CopyDescription(const CopyDescription& copyDescription)
    : filePath{copyDescription.filePath}, csvReaderConfig{nullptr}, fileType{
                                                                        copyDescription.fileType} {
    if (fileType == FileType::CSV) {
        this->csvReaderConfig = std::make_unique<CSVReaderConfig>(*copyDescription.csvReaderConfig);
    }
}

std::string CopyDescription::getFileTypeName(FileType fileType) {
    switch (fileType) {
    case FileType::CSV:
        return "csv";

    case FileType::ARROW:
        return "arrow";

    case FileType::PARQUET:
        return "parquet";
    }
}

std::string CopyDescription::getFileTypeSuffix(FileType fileType) {
    return "." + getFileTypeName(fileType);
}

void CopyDescription::setFileType(std::string const& fileName) {
    auto csvSuffix = getFileTypeSuffix(FileType::CSV);
    auto arrowSuffix = getFileTypeSuffix(FileType::ARROW);
    auto parquetSuffix = getFileTypeSuffix(FileType::PARQUET);

    if (fileName.length() >= csvSuffix.length()) {
        if (!fileName.compare(
                fileName.length() - csvSuffix.length(), csvSuffix.length(), csvSuffix)) {
            fileType = FileType::CSV;
            return;
        }
    }

    if (fileName.length() >= arrowSuffix.length()) {
        if (!fileName.compare(
                fileName.length() - arrowSuffix.length(), arrowSuffix.length(), arrowSuffix)) {
            fileType = FileType::ARROW;
            return;
        }
    }

    if (fileName.length() >= parquetSuffix.length()) {
        if (!fileName.compare(fileName.length() - parquetSuffix.length(), parquetSuffix.length(),
                parquetSuffix)) {
            fileType = FileType::PARQUET;
            return;
        }
    }

    throw CopyException("Unsupported file type: " + fileName);
}

} // namespace common
} // namespace kuzu
