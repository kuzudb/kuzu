#include "processor/operator/persistent/reader/csv/base_csv_reader.h"

#include <vector>

#include "common/data_chunk/data_chunk.h"
#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/exception/parser.h"
#include "common/string_utils.h"
#include "common/type_utils.h"
#include "common/types/blob.h"
#include "common/types/value/value.h"
#include "function/cast/numeric_cast.h"
#include "storage/store/table_copy_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

BaseCSVReader::BaseCSVReader(const std::string& filePath, const common::ReaderConfig& readerConfig)
    : filePath{filePath}, csvReaderConfig{*readerConfig.csvReaderConfig},
      expectedNumColumns(readerConfig.getNumColumns()), numColumnsDetected(-1), fd(-1),
      buffer(nullptr), bufferSize(0), position(0), rowEmpty(false), mode(ParserMode::INVALID),
      rowToAdd(0) {
    // TODO(Ziyi): should we wrap this fd using kuzu file handler?
    fd = open(filePath.c_str(), O_RDONLY
#ifdef _WIN32
                                    | _O_BINARY
#endif
    );
    if (fd == -1) {
        throw CopyException(
            StringUtils::string_format("Could not open file {}: {}", filePath, strerror(errno)));
    }
}

BaseCSVReader::~BaseCSVReader() {
    if (fd != -1) {
        close(fd);
    }
}

uint64_t BaseCSVReader::parseBlock(common::block_idx_t blockIdx, DataChunk& resultChunk) {
    currentBlockIdx = blockIdx;
    parseBlockHook();
    if (blockIdx == 0) {
        readBOM();
        if (csvReaderConfig.hasHeader) {
            readHeader();
        }
    }
    // Are we done after reading the header and executing the block hook?
    if (finishedBlockDetail()) {
        return 0;
    }
    mode = ParserMode::PARSING;
    return parseCSV(resultChunk);
}

uint64_t BaseCSVReader::countRows() {
    uint64_t rows = 0;
    readBOM();
    if (csvReaderConfig.hasHeader) {
        readHeader();
    }

line_start:
    // Pass bufferSize as start to avoid keeping any portion of the buffer.
    if (!maybeReadBuffer(nullptr)) {
        return rows;
    }

    // If the number of columns is 1, every line start indicates a row.
    if (expectedNumColumns == 1) {
        rows++;
    }

    if (buffer[position] == '\r') {
        position++;
        goto carriage_return;
    } else if (buffer[position] == '\n') {
        position++;
        goto line_start;
    } else {
        // If we have more than one column, every non-empty line is a row.
        if (expectedNumColumns != 1) {
            rows++;
        }
        goto normal;
    }
normal:
    do {
        if (buffer[position] == '\r') {
            position++;
            goto carriage_return;
        } else if (buffer[position] == '\n') {
            position++;
            goto line_start;
        } else if (buffer[position] == csvReaderConfig.quoteChar) {
            position++;
            goto in_quotes;
        } else {
            position++;
            // Just a normal character of some kind.
        }
    } while (maybeReadBuffer(nullptr));
    return rows;

carriage_return:
    if (!maybeReadBuffer(nullptr)) {
        return rows;
    }

    if (buffer[position] == '\n') {
        position++;
    }
    goto line_start;

in_quotes:
    if (!maybeReadBuffer(nullptr)) {
        return rows;
    }

    do {
        if (buffer[position] == csvReaderConfig.quoteChar) {
            position++;
            goto normal;
        } else if (buffer[position] == csvReaderConfig.escapeChar) {
            position++;
            goto escape;
        } else {
            position++;
        }
    } while (maybeReadBuffer(nullptr));
    return rows;

escape:
    if (!maybeReadBuffer(nullptr)) {
        return rows;
    }
    position++;
    goto in_quotes;
}

void BaseCSVReader::addValue(DataChunk& resultChunk, std::string_view strVal,
    const column_id_t columnIdx, std::vector<uint64_t>& escapePositions) {
    if (mode == ParserMode::PARSING_HEADER) {
        return;
    }
    auto length = strVal.length();
    if (length == 0 && columnIdx == 0) {
        rowEmpty = true;
    } else {
        rowEmpty = false;
    }
    if (expectedNumColumns != 0 && columnIdx == expectedNumColumns && length == 0) {
        // skip a single trailing delimiter in last columnIdx
        return;
    }
    if (mode == ParserMode::SNIFFING_DIALECT) {
        // Do not copy data while sniffing csv.
        return;
    }
    if (columnIdx >= expectedNumColumns) {
        throw CopyException(StringUtils::string_format(
            "Error in file {}, on line {}: expected {} values per row, but got more.", filePath,
            getLineNumber(), expectedNumColumns));
    }

    ValueVector* destination_vector = resultChunk.getValueVector(columnIdx).get();
    // insert the line number into the chunk
    if (!escapePositions.empty()) {
        // remove escape characters (if any)
        std::string newVal = "";
        uint64_t prevPos = 0;
        for (auto i = 0u; i < escapePositions.size(); i++) {
            auto nextPos = escapePositions[i];
            newVal += strVal.substr(prevPos, nextPos - prevPos);
            prevPos = nextPos + 1;
        }
        newVal += strVal.substr(prevPos, strVal.size() - prevPos);
        escapePositions.clear();
        copyStringToVector(destination_vector, std::string_view(newVal));
    } else {
        copyStringToVector(destination_vector, strVal);
    }
}

void BaseCSVReader::addRow(DataChunk& resultChunk, column_id_t column) {
    if (mode == ParserMode::PARSING_HEADER) {
        return;
    }
    if (rowEmpty) {
        rowEmpty = false;
        if (expectedNumColumns != 1) {
            return;
        }
        // Otherwise, treat it as null.
    }
    rowToAdd++;
    if (column < expectedNumColumns && mode != ParserMode::SNIFFING_DIALECT) {
        // Column number mismatch. We don't error while sniffing dialect because number of columns
        // is only known after reading the first row.
        throw CopyException(StringUtils::string_format(
            "Error in file {} on line {}: expected {} values per row, but got {}", filePath,
            getLineNumber(), expectedNumColumns, column));
    }
    if (mode == ParserMode::SNIFFING_DIALECT) {
        numColumnsDetected = column; // Use the first row to determine the number of columns.
        return;
    }
}

void BaseCSVReader::copyStringToVector(common::ValueVector* vector, std::string_view strVal) {
    auto& type = vector->dataType;
    if (strVal.empty()) {
        vector->setNull(rowToAdd, true /* isNull */);
        return;
    } else {
        vector->setNull(rowToAdd, false /* isNull */);
    }
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        int64_t val;
        function::simpleIntegerCast<int64_t>(strVal.data(), strVal.length(), val, type);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::INT32: {
        int32_t val;
        function::simpleIntegerCast<int32_t>(strVal.data(), strVal.length(), val, type);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::INT16: {
        int16_t val;
        function::simpleIntegerCast<int16_t>(strVal.data(), strVal.length(), val, type);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::INT8: {
        int8_t val;
        function::simpleIntegerCast<int8_t>(strVal.data(), strVal.length(), val, type);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::UINT64: {
        uint64_t val;
        function::simpleIntegerCast<uint64_t, false>(strVal.data(), strVal.length(), val, type);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::UINT32: {
        uint32_t val;
        function::simpleIntegerCast<uint32_t, false>(strVal.data(), strVal.length(), val, type);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::UINT16: {
        uint16_t val;
        function::simpleIntegerCast<uint16_t, false>(strVal.data(), strVal.length(), val, type);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::UINT8: {
        uint8_t val;
        function::simpleIntegerCast<uint8_t, false>(strVal.data(), strVal.length(), val, type);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::FLOAT: {
        float_t val;
        function::doubleCast<float_t>(strVal.data(), strVal.length(), val, type);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::DOUBLE: {
        double_t val;
        function::doubleCast<double_t>(strVal.data(), strVal.length(), val, type);
        vector->setValue(rowToAdd, val);
    } break;
    case LogicalTypeID::BOOL: {
        vector->setValue(rowToAdd, StringCastUtils::castToBool(strVal.data(), strVal.length()));
    } break;
    case LogicalTypeID::BLOB: {
        if (strVal.length() > BufferPoolConstants::PAGE_4KB_SIZE) {
            throw CopyException(
                ExceptionMessage::overLargeStringValueException(std::to_string(strVal.length())));
        }
        auto blobBuffer = std::make_unique<uint8_t[]>(strVal.length());
        auto blobLen = Blob::fromString(strVal.data(), strVal.length(), blobBuffer.get());
        StringVector::addString(
            vector, rowToAdd, reinterpret_cast<char*>(blobBuffer.get()), blobLen);
    } break;
    case LogicalTypeID::STRING: {
        StringVector::addString(vector, rowToAdd, strVal.data(), strVal.length());
    } break;
    case LogicalTypeID::DATE: {
        vector->setValue(rowToAdd, Date::fromCString(strVal.data(), strVal.length()));
    } break;
    case LogicalTypeID::TIMESTAMP: {
        vector->setValue(rowToAdd, Timestamp::fromCString(strVal.data(), strVal.length()));
    } break;
    case LogicalTypeID::INTERVAL: {
        vector->setValue(rowToAdd, Interval::fromCString(strVal.data(), strVal.length()));
    } break;
    case LogicalTypeID::MAP:
    case LogicalTypeID::VAR_LIST: {
        auto value = storage::TableCopyUtils::getVarListValue(
            strVal, 1, strVal.length() - 2, type, csvReaderConfig);
        vector->copyFromValue(rowToAdd, *value);
    } break;
    case LogicalTypeID::FIXED_LIST: {
        auto fixedListVal = storage::TableCopyUtils::getArrowFixedListVal(
            strVal, 1, strVal.length() - 2, type, csvReaderConfig);
        vector->copyFromValue(rowToAdd, *fixedListVal);
    } break;
    case LogicalTypeID::STRUCT: {
        auto structString = strVal.substr(1, strVal.length() - 2);
        auto structFieldIdxAndValuePairs = storage::TableCopyUtils::parseStructFieldNameAndValues(
            type, structString, csvReaderConfig);
        for (auto& fieldIdxAndValue : structFieldIdxAndValuePairs) {
            copyStringToVector(
                StructVector::getFieldVector(vector, fieldIdxAndValue.fieldIdx).get(),
                fieldIdxAndValue.fieldValue);
        }
    } break;
    case LogicalTypeID::UNION: {
        union_field_idx_t selectedFieldIdx = INVALID_STRUCT_FIELD_IDX;
        for (auto i = 0u; i < UnionType::getNumFields(&type); i++) {
            auto internalFieldIdx = UnionType::getInternalFieldIdx(i);
            if (storage::TableCopyUtils::tryCast(
                    *UnionType::getFieldType(&type, i), strVal.data(), strVal.length())) {
                StructVector::getFieldVector(vector, internalFieldIdx)
                    ->setNull(rowToAdd, false /* isNull */);
                copyStringToVector(
                    StructVector::getFieldVector(vector, internalFieldIdx).get(), strVal);
                selectedFieldIdx = i;
                break;
            } else {
                StructVector::getFieldVector(vector, internalFieldIdx)
                    ->setNull(rowToAdd, true /* isNull */);
            }
        }
        if (selectedFieldIdx == INVALID_STRUCT_FIELD_IDX) {
            throw ParserException{
                StringUtils::string_format("No parsing rule matches value: {}.", strVal)};
        }
        StructVector::getFieldVector(vector, UnionType::TAG_FIELD_IDX)
            ->setValue(rowToAdd, selectedFieldIdx);
        StructVector::getFieldVector(vector, UnionType::TAG_FIELD_IDX)
            ->setNull(rowToAdd, false /* isNull */);
    } break;
    default: { // LCOV_EXCL_START
        throw NotImplementedException("BaseCSVReader::copyStringToVector");
    } // LCOV_EXCL_STOP
    }
}

void BaseCSVReader::readBOM() {
    if (!maybeReadBuffer(nullptr)) {
        return;
    }
    if (bufferSize >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
        position = 3;
    }
}

void BaseCSVReader::readHeader() {
    mode = ParserMode::PARSING_HEADER;
    DataChunk dummyChunk(0);
    parseCSV(dummyChunk);
}

bool BaseCSVReader::readBuffer(uint64_t* start) {
    std::unique_ptr<char[]> oldBuffer = std::move(buffer);

    // the remaining part of the last buffer
    uint64_t remaining = 0;
    if (start != nullptr) {
        assert(*start <= bufferSize);
        remaining = bufferSize - *start;
    }

    uint64_t bufferReadSize = CopyConstants::INITIAL_BUFFER_SIZE;
    while (remaining > bufferReadSize) {
        bufferReadSize *= 2;
    }

    buffer = std::unique_ptr<char[]>(new char[bufferReadSize + remaining + 1]());
    bufferSize = remaining + bufferReadSize;
    if (remaining > 0) {
        // remaining from last buffer: copy it here
        assert(start != nullptr);
        memcpy(buffer.get(), oldBuffer.get() + *start, remaining);
    }

    uint64_t readCount = read(fd, buffer.get() + remaining, bufferReadSize);
    if (readCount == -1) {
        throw CopyException(StringUtils::string_format(
            "Could not read from file {}: {}", filePath, strerror(errno)));
    }

    bufferSize = remaining + readCount;
    buffer[bufferSize] = '\0';
    if (start != nullptr) {
        *start = 0;
    }
    position = remaining;
    return readCount > 0;
}

uint64_t BaseCSVReader::parseCSV(DataChunk& resultChunk) {
    // used for parsing algorithm
    rowToAdd = 0;
    column_id_t column = 0;
    uint64_t start = position;
    bool hasQuotes = false;
    std::vector<uint64_t> escapePositions;

    // read values into the buffer (if any)
    if (!maybeReadBuffer(&start)) {
        return 0;
    }

    // start parsing the first value
    goto value_start;
value_start:
    /* state: value_start */
    // this state parses the first character of a value
    if (buffer[position] == csvReaderConfig.quoteChar) {
        [[unlikely]]
        // quote: actual value starts in the next position
        // move to in_quotes state
        start = position + 1;
        hasQuotes = true;
        goto in_quotes;
    } else {
        // no quote, move to normal parsing state
        start = position;
        hasQuotes = false;
        goto normal;
    }
normal:
    /* state: normal parsing state */
    // this state parses the remainder of a non-quoted value until we reach a delimiter or
    // newline
    do {
        for (; position < bufferSize; position++) {
            if (buffer[position] == csvReaderConfig.delimiter) {
                // delimiter: end the value and add it to the chunk
                goto add_value;
            } else if (isNewLine(buffer[position])) {
                // newline: add row
                goto add_row;
            }
        }
    } while (readBuffer(&start));

    [[unlikely]]
    // file ends during normal scan: go to end state
    goto final_state;
add_value:
    // We get here after we have a delimiter.
    assert(buffer[position] == csvReaderConfig.delimiter);
    // Trim one character if we have quotes.
    addValue(resultChunk, std::string_view(buffer.get() + start, position - start - hasQuotes),
        column, escapePositions);
    column++;

    // Move past the delimiter.
    ++position;
    // Adjust start for MaybeReadBuffer.
    start = position;
    if (!maybeReadBuffer(&start)) {
        [[unlikely]]
        // File ends right after delimiter, go to final state
        goto final_state;
    }
    goto value_start;
add_row : {
    // We get here after we have a newline.
    assert(isNewLine(buffer[position]));
    bool isCarriageReturn = buffer[position] == '\r';
    addValue(resultChunk, std::string_view(buffer.get() + start, position - start - hasQuotes),
        column, escapePositions);
    column++;

    addRow(resultChunk, column);

    column = 0;
    position++;
    // Adjust start for ReadBuffer.
    start = position;
    if (!maybeReadBuffer(&start)) {
        // File ends right after newline, go to final state.
        goto final_state;
    }
    if (isCarriageReturn) {
        // \r newline, go to special state that parses an optional \n afterwards
        goto carriage_return;
    } else {
        if (finishedBlock()) {
            return rowToAdd;
        }
        goto value_start;
    }
}
in_quotes:
    // this state parses the remainder of a quoted value.
    position++;
    do {
        for (; position < bufferSize; position++) {
            if (buffer[position] == csvReaderConfig.quoteChar) {
                // quote: move to unquoted state
                goto unquote;
            } else if (buffer[position] == csvReaderConfig.escapeChar) {
                // escape: store the escaped position and move to handle_escape state
                escapePositions.push_back(position - start);
                goto handle_escape;
            } else if (isNewLine(buffer[position])) {
                [[unlikely]] handleQuotedNewline();
            }
        }
    } while (readBuffer(&start));
    [[unlikely]]
    // still in quoted state at the end of the file, error:
    throw CopyException(StringUtils::string_format(
        "Error in file {} on line {}: unterminated quotes.", filePath, getLineNumber()));
unquote:
    assert(hasQuotes && buffer[position] == csvReaderConfig.quoteChar);
    // this state handles the state directly after we unquote
    // in this state we expect either another quote (entering the quoted state again, and
    // escaping the quote) or a delimiter/newline, ending the current value and moving on to the
    // next value
    position++;
    if (!maybeReadBuffer(&start)) {
        // file ends right after unquote, go to final state
        goto final_state;
    }
    if (buffer[position] == csvReaderConfig.quoteChar &&
        (!csvReaderConfig.escapeChar || csvReaderConfig.escapeChar == csvReaderConfig.quoteChar)) {
        // escaped quote, return to quoted state and store escape position
        escapePositions.push_back(position - start);
        goto in_quotes;
    } else if (buffer[position] == csvReaderConfig.delimiter) {
        // delimiter, add value
        goto add_value;
    } else if (isNewLine(buffer[position])) {
        goto add_row;
    } else {
        [[unlikely]] throw CopyException(
            StringUtils::string_format("Error in file {} on line {}: quote should be followed by "
                                       "end of file, end of value, end of "
                                       "row or another quote.",
                filePath, getLineNumber()));
    }
handle_escape:
    /* state: handle_escape */
    // escape should be followed by a quote or another escape character
    position++;
    if (!maybeReadBuffer(&start)) {
        [[unlikely]] throw CopyException(StringUtils::string_format(
            "Error in file {} on line {}: escape at end of file.", filePath, getLineNumber()));
    }
    if (buffer[position] != csvReaderConfig.quoteChar &&
        buffer[position] != csvReaderConfig.escapeChar) {
        [[unlikely]] throw CopyException(StringUtils::string_format(
            "Error in file {} on line {}: neither QUOTE nor ESCAPE is proceeded by ESCAPE.",
            filePath, getLineNumber()));
    }
    // escape was followed by quote or escape, go back to quoted state
    goto in_quotes;
carriage_return:
    // this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted
    // as a single line

    // position points to the character after the carriage return.
    if (buffer[position] == '\n') {
        // newline after carriage return: skip
        // increase position by 1 and move start to the new position
        start = ++position;
        if (!maybeReadBuffer(&start)) {
            // file ends right after newline, go to final state
            goto final_state;
        }
    }
    if (finishedBlock()) {
        return rowToAdd;
    }

    goto value_start;
final_state:
    // We get here when the file ends.
    // If we were mid-value, add the remaining value to the chunk.
    if (position > start) {
        // Add remaining value to chunk.
        addValue(resultChunk, std::string_view(buffer.get() + start, position - start - hasQuotes),
            column, escapePositions);
        column++;
    }
    if (column > 0) {
        addRow(resultChunk, column);
    }
    return rowToAdd;
}

uint64_t BaseCSVReader::getFileOffset() const {
    uint64_t offset = lseek(fd, 0, SEEK_CUR);
    if (offset == -1) {
        // LCOV_EXCL_START
        throw CopyException(StringUtils::string_format(
            "Could not get current file position for file {}: {}", filePath, strerror(errno)));
        // LCOV_EXCL_END
    }
    assert(offset >= bufferSize);
    return offset - bufferSize + position;
}

uint64_t BaseCSVReader::getLineNumber() {
    uint64_t offset = getFileOffset();
    uint64_t lineNumber = 1;
    const uint64_t BUF_SIZE = 4096;
    char buf[BUF_SIZE];
    if (lseek(fd, 0, SEEK_SET) == -1) {
        // LCOV_EXCL_START
        throw CopyException(StringUtils::string_format(
            "Could not seek to beginning of file {}: {}", filePath, strerror(errno)));
        // LCOV_EXCL_END
    }

    bool carriageReturn = false;
    uint64_t totalBytes = 0;
    do {
        uint64_t bytesRead = read(fd, buf, std::min(BUF_SIZE, offset - totalBytes));
        if (bytesRead == -1) {
            // LCOV_EXCL_START
            throw CopyException(StringUtils::string_format(
                "Could not read from file {}: {}", filePath, strerror(errno)));
            // LCOV_EXCL_END
        }
        totalBytes += bytesRead;

        for (uint64_t i = 0; i < bytesRead; i++) {
            if (buf[i] == '\n') {
                lineNumber++;
                carriageReturn = false;
            } else if (carriageReturn) {
                lineNumber++;
                carriageReturn = false;
            }
            if (buf[i] == '\r') {
                carriageReturn = true;
            }
        }
    } while (totalBytes < offset);
    return lineNumber + carriageReturn;
}

} // namespace processor
} // namespace kuzu
