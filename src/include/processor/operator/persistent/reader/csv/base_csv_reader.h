#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include "common/copier_config/csv_reader_config.h"
#include "common/data_chunk/data_chunk.h"
#include "common/file_system/file_info.h"
#include "common/types/types.h"
#include "main/client_context.h"
#include "processor/operator/persistent/reader/csv/csv_error.h"

namespace kuzu {
namespace processor {

class BaseCSVReader {
    friend class ParsingDriver;
    friend struct SniffCSVNameAndTypeDriver;

public:
    BaseCSVReader(const std::string& filePath, common::CSVOption option, uint64_t numColumns,
        main::ClientContext* context, CSVErrorHandler* errorHandler);

    virtual ~BaseCSVReader() = default;

    virtual uint64_t parseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk) = 0;

    bool isEOF() const;
    uint64_t getFileSize();
    // Get the file offset of the current buffer position.
    uint64_t getFileOffset() const;

    std::string reconstructLine(uint64_t startPosition, uint64_t endPosition);

protected:
    template<typename Driver>
    bool addValue(Driver&, uint64_t rowNum, common::column_id_t columnIdx, std::string_view strVal,
        std::vector<uint64_t>& escapePositions);

    //! Read BOM and header.
    uint64_t handleFirstBlock();

    //! If this finds a BOM, it advances `position`.
    void readBOM();
    uint64_t readHeader();
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

    void handleCopyException(const std::string& message, bool mustThrow = false);

    template<typename Driver>
    uint64_t parseCSV(Driver&);

    inline bool isNewLine(char c) { return c == '\n' || c == '\r'; }

protected:
    virtual bool handleQuotedNewline() = 0;
    virtual uint64_t getNumRowsReadInBlock() = 0;

    void skipCurrentLine();

protected:
    common::CSVOption option;

    uint64_t numColumns;
    std::unique_ptr<common::FileInfo> fileInfo;

    common::block_idx_t currentBlockIdx;

    uint64_t rowNum;
    uint64_t numErrors;

    std::unique_ptr<char[]> buffer;
    uint64_t bufferIdx;
    uint64_t bufferSize;
    uint64_t position;
    LineContext lineContext;
    uint64_t osFileOffset;

    CSVErrorHandler* errorHandler;

    bool rowEmpty = false;
    main::ClientContext* context;
};

} // namespace processor
} // namespace kuzu
