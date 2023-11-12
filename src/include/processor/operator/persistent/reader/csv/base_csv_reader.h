#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "common/copier_config/copier_config.h"
#include "common/data_chunk/data_chunk.h"
#include "common/types/types.h"

namespace kuzu {
namespace processor {

class BaseCSVReader {
    friend class ParsingDriver;

public:
    BaseCSVReader(
        const std::string& filePath, const common::ReaderConfig& readerConfig, uint64_t numColumns);

    virtual ~BaseCSVReader();

    virtual uint64_t parseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk) = 0;

    uint64_t countRows();
    bool isEOF() const;

protected:
    template<typename Driver>
    void addValue(Driver&, uint64_t rowNum, common::column_id_t columnIdx, std::string_view strVal,
        std::vector<uint64_t>& escapePositions);

    //! Read BOM and header.
    void handleFirstBlock();

    //! If this finds a BOM, it advances `position`.
    void readBOM();
    void readHeader();
    //! Reads a new buffer from the CSV file.
    //! Uses the start value to ensure the current value stays within the buffer.
    //! Modifies the start value to point to the new start of the current value.
    //! If start is NULL, none of the buffer is kept.
    //! Returns false if the file has been exhausted.
    bool readBuffer(uint64_t* start);

    //! Like ReadBuffer, but only reads if position >= bufferSize.
    //! If this returns true, buffer[position] is a valid character that we can read.
    inline bool maybeReadBuffer(uint64_t* start) {
        return position < bufferSize || readBuffer(start);
    }

    template<typename Driver>
    uint64_t parseCSV(Driver&);

    inline bool isNewLine(char c) { return c == '\n' || c == '\r'; }

    // Get the file offset of the current buffer position.
    uint64_t getFileOffset() const;
    uint64_t getLineNumber();

protected:
    virtual void handleQuotedNewline() = 0;

protected:
    std::string filePath;
    common::CSVReaderConfig csvReaderConfig;

    uint64_t numColumns;
    int fd;

    common::block_idx_t currentBlockIdx;

    std::unique_ptr<char[]> buffer;
    uint64_t bufferSize;
    uint64_t position;

    bool rowEmpty = false;
};

} // namespace processor
} // namespace kuzu
