#pragma once

#include <fstream>

#include "common/configs.h"
#include "common/types/types_include.h"
#include "common/types/value.h"

namespace spdlog {
class logger;
}

namespace kuzu {
namespace common {

struct CSVReaderConfig {
    CSVReaderConfig()
        : escapeChar{CopyConfig::DEFAULT_CSV_ESCAPE_CHAR},
          delimiter{CopyConfig::DEFAULT_CSV_DELIMITER},
          quoteChar{CopyConfig::DEFAULT_CSV_QUOTE_CHAR},
          listBeginChar{CopyConfig::DEFAULT_CSV_LIST_BEGIN_CHAR},
          listEndChar{CopyConfig::DEFAULT_CSV_LIST_END_CHAR},
          hasHeader{CopyConfig::DEFAULT_CSV_HAS_HEADER} {}

    char escapeChar;
    char delimiter;
    char quoteChar;
    char listBeginChar;
    char listEndChar;
    bool hasHeader;
};

struct CopyDescription {
    CopyDescription(const std::string& filePath, CSVReaderConfig csvReaderConfig);

    CopyDescription(const CopyDescription& copyDescription);

    enum class FileType { CSV, ARROW, PARQUET };

    static std::string getFileTypeName(FileType fileType);

    static std::string getFileTypeSuffix(FileType fileType);

    void setFileType(std::string const& fileName);

    const std::string filePath;
    std::unique_ptr<CSVReaderConfig> csvReaderConfig;
    FileType fileType;
};

// TODO(Guodong): Remove this class and file and related code.
class CSVReader {

public:
    // Initializes to read a block in file.
    CSVReader(const std::string& fname, const CSVReaderConfig& csvReaderConfig, uint64_t blockId);
    // Initializes to read the complete file.
    CSVReader(const std::string& fname, const CSVReaderConfig& csvReaderConfig);
    // Initializes to read a part of a line.
    CSVReader(
        char* line, uint64_t lineLen, int64_t linePtrStart, const CSVReaderConfig& csvReaderConfig);

    ~CSVReader();

    // returns true if there are more lines to be parsed in a block of a CSV file, else false.
    bool hasNextLine();
    // returns true if the currently-pointed to line has more data to be parsed, else false.
    bool hasNextToken();
    bool hasNextTokenOrError();
    // Marks the currently-pointed to line as processed. hasNextLine() has to be called the move the
    // iterator to the next line.
    void skipLine();
    // Marks the currently-pointed to token as processed. hasNextToken() has to be called the move
    // the iterator to the next token of the line.
    void skipToken();
    // skips the token only if it is empty and returns the result of operation.
    bool skipTokenIfNull();

    // reads the data currently pointed to by the cursor and returns the value in a specific format.
    int64_t getInt64();
    double_t getDouble();
    uint8_t getBoolean();
    char* getString();
    date_t getDate();
    timestamp_t getTimestamp();
    interval_t getInterval();
    std::unique_ptr<Value> getList(const DataType& dataType);

private:
    void openFile(const std::string& fName);
    void setNextTokenIsProcessed();

private:
    FILE* fd;
    const CSVReaderConfig& config;
    std::shared_ptr<spdlog::logger> logger;
    bool nextLineIsNotProcessed, isEndOfBlock, nextTokenIsNotProcessed;
    char* line;
    size_t lineCapacity, lineLen;
    int64_t linePtrStart, linePtrEnd;
    size_t readingBlockStartOffset, readingBlockEndOffset;
    uint64_t nextTokenLen;
};

} // namespace common
} // namespace kuzu
