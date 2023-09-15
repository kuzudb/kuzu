#pragma once

#include <cstdint>
#include <string>

#include "common/copier_config/copier_config.h"
#include "common/data_chunk/data_chunk.h"
#include "common/types/types.h"

namespace kuzu {
namespace processor {

enum class ParserMode : uint8_t { PARSING = 0, PARSING_HEADER = 1, SNIFFING_DIALECT = 2 };

class BaseCSVReader {
public:
    BaseCSVReader(const std::string& filePath, common::CSVReaderConfig csvReaderConfig,
        uint64_t expectedNumColumns);

    virtual ~BaseCSVReader() = default;

    inline bool isNewLine(char c) { return c == '\n' || c == '\r'; }

    inline uint64_t getNumColumnsDetected() const { return numColumnsDetected; }

    common::CSVReaderConfig csvReaderConfig;
    std::string filePath;
    const uint64_t expectedNumColumns;

    uint64_t linenr = 0;
    uint64_t bytesInChunk = 0;
    bool bomChecked = false;
    bool rowEmpty = false;

    ParserMode mode;

protected:
    void AddValue(common::DataChunk& resultChunk, std::string strVal,
        common::column_id_t& columnIdx, std::vector<uint64_t>& escapePositions);
    // Adds a row to the insert_chunk, returns true if the chunk is filled as a result of this row
    // being added.
    bool AddRow(common::DataChunk& resultChunk, common::column_id_t& column);

private:
    void copyStringToVector(common::ValueVector* vector, std::string& strVal);

protected:
    uint64_t rowToAdd;
    uint64_t numColumnsDetected;
};

//! Buffered CSV reader is a class that reads values from a stream and parses them as a CSV file
class BufferedCSVReader : public BaseCSVReader {
    //! Initial buffer read size; can be extended for long lines
    static constexpr uint64_t INITIAL_BUFFER_SIZE = 16384;
    //! Larger buffer size for non disk files
    static constexpr uint64_t INITIAL_BUFFER_SIZE_LARGE = 10000000; // 10MB

public:
    BufferedCSVReader(const std::string& filePath, common::CSVReaderConfig csvReaderConfig,
        uint64_t expectedNumColumns);

    ~BufferedCSVReader() override;

    std::unique_ptr<char[]> buffer;
    uint64_t bufferSize;
    uint64_t position;
    uint64_t start = 0;
    int fd;

    std::vector<std::unique_ptr<char[]>> cachedBuffers;

public:
    //! Extract a single DataChunk from the CSV file and stores it in insert_chunk
    uint64_t ParseCSV(common::DataChunk& resultChunk);
    //! Sniffs CSV dialect and determines skip rows, header row, column types and column names
    void SniffCSV();

private:
    //! Initialize Parser
    void Initialize();
    //! Jumps back to the beginning of input stream and resets necessary internal states
    void JumpToBeginning(bool skipHeader);
    //! Skips skip_rows, reads header row from input stream
    void ReadHeader();
    //! Resets the buffer
    void ResetBuffer();
    //! Extract a single DataChunk from the CSV file and stores it in insert_chunk
    uint64_t TryParseCSV(common::DataChunk& resultChunk, std::string& errorMessage);
    //! Parses a CSV file with a one-byte delimiter, escape and quote character
    uint64_t TryParseSimpleCSV(common::DataChunk& resultChunk, std::string& errorMessage);
    //! Reads a new buffer from the CSV file if the current one has been exhausted
    bool ReadBuffer(uint64_t& start, uint64_t& lineStart);
    //! Skip Empty lines for tables with over one column
    void SkipEmptyLines();
};

} // namespace processor
} // namespace kuzu
