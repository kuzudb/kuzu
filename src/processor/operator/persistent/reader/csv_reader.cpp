#include "processor/operator/persistent/reader/csv_reader.h"

#include <vector>

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/exception/not_implemented.h"
#include "common/exception/parser.h"
#include "common/string_utils.h"
#include "common/types/blob.h"
#include "storage/copier/table_copy_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

BaseCSVReader::BaseCSVReader(const std::string& filePath, common::CSVReaderConfig csvReaderConfig,
    catalog::TableSchema* tableSchema)
    : csvReaderConfig{std::move(csvReaderConfig)}, filePath{filePath},
      tableSchema{tableSchema}, rowToAdd{0} {}

void BaseCSVReader::AddValue(DataChunk& resultChunk, std::string strVal, column_id_t& columnIdx,
    std::vector<uint64_t>& escapePositions) {
    if (mode == ParserMode::PARSING_HEADER) {
        return;
    }
    auto length = strVal.length();
    if (length == 0 && columnIdx == 0) {
        rowEmpty = true;
    } else {
        rowEmpty = false;
    }
    if (numPropertiesToCopy != 0 && columnIdx == numPropertiesToCopy && length == 0) {
        // skip a single trailing delimiter in last columnIdx
        return;
    }
    if (columnIdx >= numPropertiesToCopy) {
        throw CopyException(StringUtils::string_format(
            "Error in file {}, on line {}: expected {} values per row, but got more. ", filePath,
            linenr, numPropertiesToCopy));
    }

    // insert the line number into the chunk
    if (!escapePositions.empty()) {
        // remove escape characters (if any)
        std::string oldVal = strVal;
        std::string newVal = "";
        uint64_t prevPos = 0;
        for (auto i = 0u; i < escapePositions.size(); i++) {
            auto nextPos = escapePositions[i];
            newVal += oldVal.substr(prevPos, nextPos - prevPos);
            prevPos = nextPos + 1;
        }
        newVal += oldVal.substr(prevPos, oldVal.size() - prevPos);
        escapePositions.clear();
        strVal = newVal;
    }
    copyStringToVector(resultChunk.getValueVector(columnIdx).get(), strVal);
    // move to the next columnIdx
    columnIdx++;
}

bool BaseCSVReader::AddRow(DataChunk& resultChunk, column_id_t& column) {
    rowToAdd++;
    if (mode == ParserMode::PARSING_HEADER) {
        return true;
    }
    linenr++;
    if (rowEmpty) {
        rowEmpty = false;
        if (numPropertiesToCopy != 1) {
            if (mode == ParserMode::PARSING) {
                // Set This position to be null
                ;
            }
            column = 0;
            return false;
        }
    }
    if (column < numPropertiesToCopy) {
        throw CopyException(StringUtils::string_format(
            "Error in file {} on line {}: expected {} values per row, but got {}", filePath, linenr,
            numPropertiesToCopy, column));
    }
    if (mode == ParserMode::PARSING && rowToAdd >= DEFAULT_VECTOR_CAPACITY) {
        return true;
    }

    column = 0;
    return false;
}

void BaseCSVReader::copyStringToVector(common::ValueVector* vector, std::string& strVal) {
    auto& type = vector->dataType;
    if (strVal.empty()) {
        vector->setNull(rowToAdd, true /* isNull */);
        return;
    } else {
        vector->setNull(rowToAdd, false /* isNull */);
    }
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        vector->setValue(
            rowToAdd, StringCastUtils::castToNum<int64_t>(strVal.c_str(), strVal.length()));
    } break;
    case LogicalTypeID::INT32: {
        vector->setValue(
            rowToAdd, StringCastUtils::castToNum<int32_t>(strVal.c_str(), strVal.length()));
    } break;
    case LogicalTypeID::INT16: {
        vector->setValue(
            rowToAdd, StringCastUtils::castToNum<int16_t>(strVal.c_str(), strVal.length()));
    } break;
    case LogicalTypeID::INT8: {
        vector->setValue(
            rowToAdd, StringCastUtils::castToNum<int8_t>(strVal.c_str(), strVal.length()));
    } break;
    case LogicalTypeID::FLOAT: {
        vector->setValue(
            rowToAdd, StringCastUtils::castToNum<float_t>(strVal.c_str(), strVal.length()));
    } break;
    case LogicalTypeID::DOUBLE: {
        vector->setValue(
            rowToAdd, StringCastUtils::castToNum<double_t>(strVal.c_str(), strVal.length()));
    } break;
    case LogicalTypeID::BOOL: {
        vector->setValue(rowToAdd, StringCastUtils::castToBool(strVal.c_str(), strVal.length()));
    } break;
    case LogicalTypeID::BLOB: {
        if (strVal.length() > BufferPoolConstants::PAGE_4KB_SIZE) {
            throw CopyException(
                ExceptionMessage::overLargeStringValueException(std::to_string(strVal.length())));
        }
        auto blobBuffer = std::make_unique<uint8_t[]>(strVal.length());
        auto blobLen = Blob::fromString(strVal.c_str(), strVal.length(), blobBuffer.get());
        StringVector::addString(
            vector, rowToAdd, reinterpret_cast<char*>(blobBuffer.get()), blobLen);
    } break;
    case LogicalTypeID::STRING: {
        StringVector::addString(vector, rowToAdd, strVal.c_str(), strVal.length());
    } break;
    case LogicalTypeID::DATE: {
        vector->setValue(rowToAdd, Date::fromCString(strVal.c_str(), strVal.length()));
    } break;
    case LogicalTypeID::TIMESTAMP: {
        vector->setValue(rowToAdd, Timestamp::fromCString(strVal.c_str(), strVal.length()));
    } break;
    case LogicalTypeID::INTERVAL: {
        vector->setValue(rowToAdd, Interval::fromCString(strVal.c_str(), strVal.length()));
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
                    *UnionType::getFieldType(&type, i), strVal.c_str(), strVal.length())) {
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
    default: {
        throw NotImplementedException("BaseCSVReader::AddValue");
    }
    }
}

BufferedCSVReader::BufferedCSVReader(const std::string& filePath,
    common::CSVReaderConfig csvReaderConfig, catalog::TableSchema* tableSchema)
    : BaseCSVReader{filePath, csvReaderConfig, tableSchema}, bufferSize{0}, position{0}, start{0} {
    Initialize();
}

BufferedCSVReader::~BufferedCSVReader() {
    if (fd != -1) {
        close(fd);
    }
}

void BufferedCSVReader::Initialize() {
    numPropertiesToCopy = 0;
    for (auto& property : tableSchema->properties) {
        if (property->getDataType()->getLogicalTypeID() != common::LogicalTypeID::SERIAL) {
            numPropertiesToCopy++;
        }
    }
    // TODO(Ziyi): should we wrap this fd using kuzu file handler?
    fd = open(filePath.c_str(), O_RDONLY);
    ResetBuffer();
    if (csvReaderConfig.hasHeader) {
        ReadHeader();
    }
    mode = ParserMode::PARSING;
}

void BufferedCSVReader::ResetBuffer() {
    buffer.reset();
    bufferSize = 0;
    position = 0;
    start = 0;
    cachedBuffers.clear();
}

void BufferedCSVReader::ReadHeader() {
    // ignore the first line as a header line
    mode = ParserMode::PARSING_HEADER;
    DataChunk dummy_chunk(0);
    ParseCSV(dummy_chunk);
}

bool BufferedCSVReader::ReadBuffer(uint64_t& start, uint64_t& lineStart) {
    if (start > bufferSize) {
        return false;
    }
    auto oldBuffer = std::move(buffer);

    // the remaining part of the last buffer
    uint64_t remaining = bufferSize - start;

    uint64_t bufferReadSize = INITIAL_BUFFER_SIZE;

    while (remaining > bufferReadSize) {
        bufferReadSize *= 2;
    }

    buffer = std::unique_ptr<char[]>(new char[bufferReadSize + remaining + 1]());
    bufferSize = remaining + bufferReadSize;
    if (remaining > 0) {
        // remaining from last buffer: copy it here
        memcpy(buffer.get(), oldBuffer.get() + start, remaining);
    }

    uint64_t readCount = read(fd, buffer.get() + remaining, bufferReadSize);
    if (readCount == -1) {
        throw CopyException(StringUtils::string_format(
            "Could not read from file {}: {}", filePath, strerror(errno)));
    }

    bytesInChunk += readCount;
    bufferSize = remaining + readCount;
    buffer[bufferSize] = '\0';
    if (oldBuffer) {
        cachedBuffers.push_back(std::move(oldBuffer));
    }
    start = 0;
    position = remaining;
    if (!bomChecked) {
        bomChecked = true;
        if (readCount >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
            start += 3;
            position += 3;
        }
    }
    lineStart = start;

    return readCount > 0;
}

void BufferedCSVReader::SkipEmptyLines() {
    if (numPropertiesToCopy == 1) {
        // Empty lines are null data.
        return;
    }
    for (; position < bufferSize; position++) {
        if (!isNewLine(buffer[position])) {
            return;
        }
    }
}

uint64_t BufferedCSVReader::TryParseSimpleCSV(DataChunk& resultChunk, std::string& errorMessage) {
    // used for parsing algorithm
    rowToAdd = 0;
    bool finishedChunk = false;
    column_id_t column = 0;
    uint64_t offset = 0;
    bool hasQuotes = false;
    std::vector<uint64_t> escapePositions;

    uint64_t lineStart = position;
    // read values into the buffer (if any)
    if (position >= bufferSize) {
        if (!ReadBuffer(start, lineStart)) {
            return 0;
        }
    }

    // start parsing the first value
    goto value_start;
value_start:
    offset = 0;
    /* state: value_start */
    // this state parses the first character of a value
    if (buffer[position] == csvReaderConfig.quoteChar) {
        // quote: actual value starts in the next position
        // move to in_quotes state
        start = position + 1;
        goto in_quotes;
    } else {
        // no quote, move to normal parsing state
        start = position;
        goto normal;
    }
normal:
    /* state: normal parsing state */
    // this state parses the remainder of a non-quoted value until we reach a delimiter or newline
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
    } while (ReadBuffer(start, lineStart));
    // file ends during normal scan: go to end state
    goto final_state;
add_value:
    AddValue(resultChunk, std::string(buffer.get() + start, position - start - offset), column,
        escapePositions);
    // increase position by 1 and move start to the new position
    offset = 0;
    hasQuotes = false;
    start = ++position;
    if (position >= bufferSize && !ReadBuffer(start, lineStart)) {
        // file ends right after delimiter, go to final state
        goto final_state;
    }
    goto value_start;
add_row : {
    // check type of newline (\r or \n)
    bool carriageReturn = buffer[position] == '\r';
    AddValue(resultChunk, std::string(buffer.get() + start, position - start - offset), column,
        escapePositions);
    if (!errorMessage.empty()) {
        return -1;
    }
    finishedChunk = AddRow(resultChunk, column);
    if (!errorMessage.empty()) {
        return -1;
    }
    // increase position by 1 and move start to the new position
    offset = 0;
    hasQuotes = false;
    position++;
    start = position;
    lineStart = position;
    if (position >= bufferSize && !ReadBuffer(start, lineStart)) {
        // file ends right after delimiter, go to final state
        goto final_state;
    }
    if (carriageReturn) {
        // \r newline, go to special state that parses an optional \n afterwards
        goto carriage_return;
    } else {
        SkipEmptyLines();
        start = position;
        lineStart = position;
        if (position >= bufferSize && !ReadBuffer(start, lineStart)) {
            // file ends right after delimiter, go to final state
            goto final_state;
        }
        // \n newline, move to value start
        if (finishedChunk) {
            return rowToAdd;
        }
        goto value_start;
    }
}
in_quotes:
    /* state: in_quotes */
    // this state parses the remainder of a quoted value
    hasQuotes = true;
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
            }
        }
    } while (ReadBuffer(start, lineStart));
    // still in quoted state at the end of the file, error:
    throw CopyException(StringUtils::string_format(
        "Error in file {} on line {}: unterminated quotes. ", filePath, linenr));
unquote:
    /* state: unquote */
    // this state handles the state directly after we unquote
    // in this state we expect either another quote (entering the quoted state again, and escaping
    // the quote) or a delimiter/newline, ending the current value and moving on to the next value
    position++;
    if (position >= bufferSize && !ReadBuffer(start, lineStart)) {
        // file ends right after unquote, go to final state
        offset = 1;
        goto final_state;
    }
    if (buffer[position] == csvReaderConfig.quoteChar &&
        (!csvReaderConfig.escapeChar || csvReaderConfig.escapeChar == csvReaderConfig.quoteChar)) {
        // escaped quote, return to quoted state and store escape position
        escapePositions.push_back(position - start);
        goto in_quotes;
    } else if (buffer[position] == csvReaderConfig.delimiter) {
        // delimiter, add value
        offset = 1;
        goto add_value;
    } else if (isNewLine(buffer[position])) {
        offset = 1;
        goto add_row;
    } else {
        errorMessage = StringUtils::string_format(
            "Error in file {} on line {}: quote should be followed by end of value, end of "
            "row or another quote.",
            filePath, linenr);
        return -1;
    }
handle_escape:
    /* state: handle_escape */
    // escape should be followed by a quote or another escape character
    position++;
    if (position >= bufferSize && !ReadBuffer(start, lineStart)) {
        errorMessage = StringUtils::string_format(
            "Error in file {} on line {}: neither QUOTE nor ESCAPE is proceeded by ESCAPE.",
            filePath, linenr);
        return -1;
    }
    if (buffer[position] != csvReaderConfig.quoteChar &&
        buffer[position] != csvReaderConfig.escapeChar) {
        errorMessage = StringUtils::string_format(
            "Error in file {} on line {}}: neither QUOTE nor ESCAPE is proceeded by ESCAPE.",
            filePath, linenr);
        return -1;
    }
    // escape was followed by quote or escape, go back to quoted state
    goto in_quotes;
carriage_return:
    /* state: carriage_return */
    // this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted as
    // a single line
    if (buffer[position] == '\n') {
        // newline after carriage return: skip
        // increase position by 1 and move start to the new position
        start = ++position;
        if (position >= bufferSize && !ReadBuffer(start, lineStart)) {
            // file ends right after delimiter, go to final state
            goto final_state;
        }
    }
    if (finishedChunk) {
        return rowToAdd;
    }
    SkipEmptyLines();
    start = position;
    lineStart = position;
    if (position >= bufferSize && !ReadBuffer(start, lineStart)) {
        // file ends right after delimiter, go to final state
        goto final_state;
    }

    goto value_start;
final_state:
    if (finishedChunk) {
        return rowToAdd;
    }

    if (column > 0 || position > start) {
        // remaining values to be added to the chunk
        AddValue(resultChunk, std::string(buffer.get() + start, position - start - offset), column,
            escapePositions);
        finishedChunk = AddRow(resultChunk, column);
        SkipEmptyLines();
        if (!errorMessage.empty()) {
            return -1;
        }
    }

    return rowToAdd;
}

uint64_t BufferedCSVReader::ParseCSV(common::DataChunk& resultChunk) {
    std::string errorMessage;
    auto numRowsRead = TryParseCSV(resultChunk, errorMessage);
    if (numRowsRead == -1) {
        throw CopyException(errorMessage);
    }
    resultChunk.state->selVector->selectedSize = numRowsRead;
    return numRowsRead;
}

uint64_t BufferedCSVReader::TryParseCSV(DataChunk& resultChunk, std::string& errorMessage) {
    return TryParseSimpleCSV(resultChunk, errorMessage);
}

} // namespace processor
} // namespace kuzu
