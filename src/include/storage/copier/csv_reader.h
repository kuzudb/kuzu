#pragma once

#include <cstdint>
#include <string>

#include "catalog/catalog.h"
#include "common/copier_config/copier_config.h"
#include "common/data_chunk/data_chunk.h"
#include "common/types/types.h"

namespace kuzu {
namespace storage {

enum class ParserMode : uint8_t { PARSING = 0, PARSING_HEADER = 1 };

class BaseCSVReader {
public:
    BaseCSVReader(const std::string& filePath, common::CSVReaderConfig* csvReaderConfig,
                  catalog::TableSchema* tableSchema);
    virtual ~BaseCSVReader();

    common::CSVReaderConfig* csvReaderConfig;
    std::string filePath;
    catalog::TableSchema* tableSchema;
    std::vector<common::LogicalType> return_types;

    uint64_t linenr = 0;
    uint64_t bytes_in_chunk = 0;
    bool bom_checked = false;
    bool row_empty = false;
    bool end_of_file_reached = false;

    ParserMode mode;

protected:
    void AddValue(std::string str_val, common::column_id_t &column, std::vector<uint64_t> &escape_positions, bool has_quotes,
            uint64_t buffer_idx = 0);
    //! Adds a row to the insert_chunk, returns true if the chunk is filled as a result of this row being added
    bool AddRow(common::DataChunk &insert_chunk, common::column_id_t &column, std::string &error_message, uint64_t buffer_idx = 0);
    //! Finalizes a chunk, parsing all values that have been added so far and adding them to the insert_chunk
    bool Flush(common::DataChunk &insert_chunk, uint64_t buffer_idx = 0, bool try_add_line = false);

// void InitParseChunk(common::column_id_t num_cols);
};

//! Buffered CSV reader is a class that reads values from a stream and parses them as a CSV file
class BufferedCSVReader : public BaseCSVReader {
    //! Initial buffer read size; can be extended for long lines
    static constexpr uint64_t INITIAL_BUFFER_SIZE = 16384;
    //! Larger buffer size for non disk files
    static constexpr uint64_t INITIAL_BUFFER_SIZE_LARGE = 10000000; // 10MB

public:
    BufferedCSVReader(const std::string& filePath, common::CSVReaderConfig* csvReaderConfig,
                          catalog::TableSchema* tableSchema);
    virtual ~BufferedCSVReader() {
    }
    std::unique_ptr<char[]> buffer;
    uint64_t bufferSize;
    uint64_t position;
    uint64_t start = 0;

    std::vector<std::unique_ptr<char[]>> cachedBuffers;

    std::string filePath;

public:
    //! Extract a single DataChunk from the CSV file and stores it in insert_chunk
    void ParseCSV(common::DataChunk &insertChunk);

private:
    //! Initialize Parser
    void Initialize(const std::vector<catalog::Property*> properties);
    //! Skips skip_rows, reads header row from input stream
    void ReadHeader(bool hasHeader);
    //! Resets the buffer
    void ResetBuffer();
    //! Extract a single DataChunk from the CSV file and stores it in insert_chunk
    bool TryParseCSV(ParserMode mode, common::DataChunk &insertChunk, std::string &errorMessage);
    //! Parses a CSV file with a one-byte delimiter, escape and quote character
    bool TryParseSimpleCSV(common::DataChunk &insertChunk, std::string &errorMessage);
    //! Reads a new buffer from the CSV file if the current one has been exhausted
    bool ReadBuffer(uint64_t &start, uint64_t &line_start);
    //! Skip Empty lines for tables with over one column
    void SkipEmptyLines();
};

} // namespace storage
} // namespace kuzu