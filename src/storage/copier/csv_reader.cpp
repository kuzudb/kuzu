#include "storage/copier/csv_reader.h"

#include <vector>

#include "common/string_utils.h"
#include "storage/copier/table_copy_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace storage {

BaseCSVReader::BaseCSVReader(const std::string& filePath, common::CSVReaderConfig* csvReaderConfig,
    catalog::TableSchema* tableSchema)
    : csvReaderConfig(csvReaderConfig), filePath(filePath), tableSchema(tableSchema),
      parse_chunk(DataChunk{tableSchema->getNumProperties()}), rowToAdd(0) {}

BaseCSVReader::~BaseCSVReader() {}

void BaseCSVReader::AddValue(std::string str_val, column_id_t& column,
    std::vector<uint64_t>& escape_positions, bool has_quotes, uint64_t buffer_idx) {
    if (mode == ParserMode::PARSING_HEADER) {
        return;
    }
    auto length = str_val.length();
    if (length == 0 && column == 0) {
        row_empty = true;
    } else {
        row_empty = false;
    }
    if (!return_types.empty() && column == return_types.size() && length == 0) {
        // skip a single trailing delimiter in last column
        return;
    }
    if (column >= return_types.size()) {
        throw CopyException(StringUtils::string_format(
            "Error in file {}, on line {}: expected {} values per row, but got more. ", filePath,
            linenr, return_types.size()));
    }

    // insert the line number into the chunk
    if (!escape_positions.empty()) {
        // remove escape characters (if any)
        std::string old_val = str_val;
        std::string new_val = "";
        uint64_t prev_pos = 0;
        for (uint64_t i = 0; i < escape_positions.size(); i++) {
            uint64_t next_pos = escape_positions[i];
            new_val += old_val.substr(prev_pos, next_pos - prev_pos);
            prev_pos = next_pos + 1;
        }
        new_val += old_val.substr(prev_pos, old_val.size() - prev_pos);
        escape_positions.clear();
        str_val = new_val;
    }
    auto type = parse_chunk.getValueVector(column)->dataType;
    switch (type.getLogicalTypeID()) {
    case LogicalTypeID::INT64: {
        parse_chunk.getValueVector(column)->setValue(
            rowToAdd, StringCastUtils::castToNum<int64_t>(str_val.c_str(), str_val.length()));
    } break;
    case LogicalTypeID::INT32: {
        parse_chunk.getValueVector(column)->setValue(
            rowToAdd, StringCastUtils::castToNum<int32_t>(str_val.c_str(), str_val.length()));
    } break;
    case LogicalTypeID::INT16: {
        parse_chunk.getValueVector(column)->setValue(
            rowToAdd, StringCastUtils::castToNum<int16_t>(str_val.c_str(), str_val.length()));
    } break;
    case LogicalTypeID::FLOAT: {
        parse_chunk.getValueVector(column)->setValue(
            rowToAdd, StringCastUtils::castToNum<float_t>(str_val.c_str(), str_val.length()));
    } break;
    case LogicalTypeID::DOUBLE: {
        parse_chunk.getValueVector(column)->setValue(
            rowToAdd, StringCastUtils::castToNum<double_t>(str_val.c_str(), str_val.length()));
    } break;
    case LogicalTypeID::BOOL: {
        parse_chunk.getValueVector(column)->setValue(
            rowToAdd, StringCastUtils::castToBool(str_val.c_str(), str_val.length()));
    } break;
    case LogicalTypeID::STRING: {
        parse_chunk.getValueVector(column)->setValue(rowToAdd, str_val);
    } break;
    case LogicalTypeID::DATE: {
        parse_chunk.getValueVector(column)->setValue(
            rowToAdd, Date::fromCString(str_val.c_str(), str_val.length()));
    } break;
    case LogicalTypeID::TIMESTAMP: {
        parse_chunk.getValueVector(column)->setValue(
            rowToAdd, Timestamp::fromCString(str_val.c_str(), str_val.length()));
    } break;
    case LogicalTypeID::INTERVAL: {
        parse_chunk.getValueVector(column)->setValue(
            rowToAdd, Interval::fromCString(str_val.c_str(), str_val.length()));
    } break;
    case LogicalTypeID::VAR_LIST: {
        auto copyDescription =
            CopyDescription({filePath}, common::CopyDescription::FileType::CSV, *csvReaderConfig);
        auto value = TableCopyUtils::getVarListValue(
            str_val, 1, str_val.length() - 2, type, copyDescription);
        ValueVector::copyValueToVector(parse_chunk.getValueVector(column)->getData(),
            parse_chunk.getValueVector(column).get(), value.get());
    } break;
    case LogicalTypeID::FIXED_LIST: {
        auto copyDescription =
            CopyDescription({filePath}, common::CopyDescription::FileType::CSV, *csvReaderConfig);
        auto value = TableCopyUtils::getArrowFixedListVal(
            str_val, 1, str_val.length() - 2, type, copyDescription);
        ValueVector::copyValueToVector(parse_chunk.getValueVector(column)->getData(),
            parse_chunk.getValueVector(column).get() + rowToAdd, value.get());
    } break;
    default:
        throw NotImplementedException(
            "Unsupported data type " + LogicalTypeUtils::dataTypeToString(type.getLogicalTypeID()));
    }
    parse_chunk.getValueVector(column)->state->selVector->selectedSize++;
    // move to the next column
    column++;
}

bool BaseCSVReader::AddRow(
    DataChunk& insert_chunk, column_id_t& column, std::string& error_message, uint64_t buffer_idx) {
    rowToAdd++;
    if (mode == ParserMode::PARSING_HEADER) {
        return true;
    }
    linenr++;
    if (row_empty) {
        row_empty = false;
        if (return_types.size() != 1) {
            if (mode == ParserMode::PARSING) {
                // Set This position to be null
                ;
            }
            column = 0;
            return false;
        }
    }
    if (column < return_types.size()) {
        throw CopyException(StringUtils::string_format(
            "Error in file {} on line {}: expected {} values per row, but got {}", filePath, linenr,
            return_types.size(), column));
    }
    if (mode == ParserMode::PARSING && rowToAdd >= common::StorageConstants::NODE_GROUP_SIZE) {
        Flush(insert_chunk, buffer_idx);
        return true;
    }

    column = 0;
    return false;
}

bool BaseCSVReader::Flush(DataChunk& insert_chunk, uint64_t buffer_idx, bool try_add_line) {
    if (parse_chunk.getNumValueVectors() == 0) {
        return true;
    }
    for (uint64_t c = 0; c < parse_chunk.getNumValueVectors(); c++) {
        auto parse_vector = parse_chunk.getValueVector(c);
        auto result_vector = insert_chunk.getValueVector(c);
        insert_chunk.getValueVector(c)->state->selVector->selectedSize =
            parse_chunk.getValueVector(c)->state->selVector->selectedSize;
        insert_chunk.insert(c, parse_chunk.getValueVector(c));
    }
    InitParseChunk(parse_chunk.getNumValueVectors());
    return true;
}

void BaseCSVReader::InitParseChunk(column_id_t numCols) {
    parse_chunk = DataChunk(numCols);
}

BufferedCSVReader::BufferedCSVReader(const std::string& filePath,
    common::CSVReaderConfig* csvReaderConfig, catalog::TableSchema* tableSchema,
    storage::MemoryManager* memoryManager = nullptr)
    : BaseCSVReader(filePath, csvReaderConfig, tableSchema), bufferSize(0), position(0), start(0) {
    Initialize(tableSchema->getProperties(), memoryManager);
}

void BufferedCSVReader::Initialize(
    std::vector<kuzu::catalog::Property*> properties, storage::MemoryManager* memoryManager) {
    // PrepareComplexParser();
    for (auto property : properties) {
        return_types.push_back(*property->getDataType());
    }
    fd = open(filePath.c_str(), O_RDONLY);
    ResetBuffer();
    if (csvReaderConfig->hasHeader) {
        ReadHeader();
    }
    mode = ParserMode::PARSING;
    // Initializing parse_chunk DataChunk
    parse_chunk = DataChunk(properties.size());
    for (int i = 0; i < properties.size(); i++) {
        auto type = properties[i]->getDataType();
        auto typeID = type->getLogicalTypeID();
        auto v = std::make_shared<ValueVector>(*type, memoryManager);
        parse_chunk.insert(i, v);
    }
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

bool BufferedCSVReader::ReadBuffer(uint64_t& start, uint64_t& line_start) {
    if (start > bufferSize) {
        return false;
    }
    auto old_buffer = std::move(buffer);

    // the remaining part of the last buffer
    uint64_t remaining = bufferSize - start;

    uint64_t buffer_read_size = INITIAL_BUFFER_SIZE;

    while (remaining > buffer_read_size) {
        buffer_read_size *= 2;
    }

    buffer = std::unique_ptr<char[]>(new char[buffer_read_size + remaining + 1]());
    bufferSize = remaining + buffer_read_size;
    if (remaining > 0) {
        // remaining from last buffer: copy it here
        memcpy(buffer.get(), old_buffer.get() + start, remaining);
    }

    uint64_t read_count = read(fd, buffer.get() + remaining, buffer_read_size);
    if (read_count == -1) {
        throw CopyException(StringUtils::string_format(
            "Could not read from file {}: {}", filePath, strerror(errno)));
    }

    bytes_in_chunk += read_count;
    bufferSize = remaining + read_count;
    buffer[bufferSize] = '\0';
    if (old_buffer) {
        cachedBuffers.push_back(std::move(old_buffer));
    }
    start = 0;
    position = remaining;
    if (!bom_checked) {
        bom_checked = true;
        if (read_count >= 3 && buffer[0] == '\xEF' && buffer[1] == '\xBB' && buffer[2] == '\xBF') {
            start += 3;
            position += 3;
        }
    }
    line_start = start;

    return read_count > 0;
}

void BufferedCSVReader::SkipEmptyLines() {
    for (; position < bufferSize; position++) {
        if (!StringUtils::CharacterIsNewline(buffer[position])) {
            return;
        }
    }
}

uint64_t BufferedCSVReader::TryParseSimpleCSV(DataChunk& insertChunk, std::string& errorMessage) {
    // used for parsing algorithm
    bool finished_chunk = false;
    column_id_t column = 0;
    uint64_t offset = 0;
    bool has_quotes = false;
    std::vector<uint64_t> escape_positions;

    uint64_t line_start = position;
    // read values into the buffer (if any)
    if (position >= bufferSize) {
        if (!ReadBuffer(start, line_start)) {
            return 0;
        }
    }

    // start parsing the first value
    goto value_start;
value_start:
    offset = 0;
    /* state: value_start */
    // this state parses the first character of a value
    if (buffer[position] == csvReaderConfig->quoteChar) {
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
            if (buffer[position] == csvReaderConfig->delimiter) {
                // delimiter: end the value and add it to the chunk
                goto add_value;
            } else if (StringUtils::CharacterIsNewline(buffer[position])) {
                // newline: add row
                goto add_row;
            }
        }
    } while (ReadBuffer(start, line_start));
    // file ends during normal scan: go to end state
    goto final_state;
add_value:
    AddValue(std::string(buffer.get() + start, position - start - offset), column, escape_positions,
        has_quotes);
    // increase position by 1 and move start to the new position
    offset = 0;
    has_quotes = false;
    start = ++position;
    if (position >= bufferSize && !ReadBuffer(start, line_start)) {
        // file ends right after delimiter, go to final state
        goto final_state;
    }
    goto value_start;
add_row : {
    // check type of newline (\r or \n)
    bool carriage_return = buffer[position] == '\r';
    AddValue(std::string(buffer.get() + start, position - start - offset), column, escape_positions,
        has_quotes);
    if (!errorMessage.empty()) {
        return -1;
    }
    finished_chunk = AddRow(insertChunk, column, errorMessage);
    if (!errorMessage.empty()) {
        return -1;
    }
    // increase position by 1 and move start to the new position
    offset = 0;
    has_quotes = false;
    position++;
    start = position;
    line_start = position;
    if (position >= bufferSize && !ReadBuffer(start, line_start)) {
        // file ends right after delimiter, go to final state
        goto final_state;
    }
    if (carriage_return) {
        // \r newline, go to special state that parses an optional \n afterwards
        goto carriage_return;
    } else {
        SkipEmptyLines();
        start = position;
        line_start = position;
        if (position >= bufferSize && !ReadBuffer(start, line_start)) {
            // file ends right after delimiter, go to final state
            goto final_state;
        }
        // \n newline, move to value start
        if (finished_chunk) {
            return rowToAdd;
        }
        goto value_start;
    }
}
in_quotes:
    /* state: in_quotes */
    // this state parses the remainder of a quoted value
    has_quotes = true;
    position++;
    do {
        for (; position < bufferSize; position++) {
            if (buffer[position] == csvReaderConfig->quoteChar) {
                // quote: move to unquoted state
                goto unquote;
            } else if (buffer[position] == csvReaderConfig->escapeChar) {
                // escape: store the escaped position and move to handle_escape state
                escape_positions.push_back(position - start);
                goto handle_escape;
            }
        }
    } while (ReadBuffer(start, line_start));
    // still in quoted state at the end of the file, error:
    throw CopyException(StringUtils::string_format(
        "Error in file {} on line {}: unterminated quotes. ", filePath, linenr));
unquote:
    /* state: unquote */
    // this state handles the state directly after we unquote
    // in this state we expect either another quote (entering the quoted state again, and escaping
    // the quote) or a delimiter/newline, ending the current value and moving on to the next value
    position++;
    if (position >= bufferSize && !ReadBuffer(start, line_start)) {
        // file ends right after unquote, go to final state
        offset = 1;
        goto final_state;
    }
    if (buffer[position] == csvReaderConfig->quoteChar &&
        (!csvReaderConfig->escapeChar ||
            csvReaderConfig->escapeChar == csvReaderConfig->quoteChar)) {
        // escaped quote, return to quoted state and store escape position
        escape_positions.push_back(position - start);
        goto in_quotes;
    } else if (buffer[position] == csvReaderConfig->delimiter) {
        // delimiter, add value
        offset = 1;
        goto add_value;
    } else if (StringUtils::CharacterIsNewline(buffer[position])) {
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
    if (position >= bufferSize && !ReadBuffer(start, line_start)) {
        errorMessage = StringUtils::string_format(
            "Error in file {} on line {}: neither QUOTE nor ESCAPE is proceeded by ESCAPE.",
            filePath, linenr);
        return -1;
    }
    if (buffer[position] != csvReaderConfig->quoteChar &&
        buffer[position] != csvReaderConfig->escapeChar) {
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
        if (position >= bufferSize && !ReadBuffer(start, line_start)) {
            // file ends right after delimiter, go to final state
            goto final_state;
        }
    }
    if (finished_chunk) {
        return rowToAdd;
    }
    SkipEmptyLines();
    start = position;
    line_start = position;
    if (position >= bufferSize && !ReadBuffer(start, line_start)) {
        // file ends right after delimiter, go to final state
        goto final_state;
    }

    goto value_start;
final_state:
    if (finished_chunk) {
        return rowToAdd;
    }

    if (column > 0 || position > start) {
        // remaining values to be added to the chunk
        AddValue(std::string(buffer.get() + start, position - start - offset), column,
            escape_positions, has_quotes);
        finished_chunk = AddRow(insertChunk, column, errorMessage);
        SkipEmptyLines();
        if (!errorMessage.empty()) {
            return -1;
        }
    }

    // final stage, only reached after parsing the file is finished
    // flush the parsed chunk and finalize parsing
    if (mode == ParserMode::PARSING) {
        Flush(insertChunk);
    }

    end_of_file_reached = true;
    return rowToAdd;
}

uint64_t BufferedCSVReader::ParseCSV(common::DataChunk& insertChunk) {
    std::string errorMessage;
    auto numRowsRead = TryParseCSV(insertChunk, errorMessage);
    if (numRowsRead == -1) {
        throw CopyException(errorMessage);
    }
    return numRowsRead;
}

uint64_t BufferedCSVReader::TryParseCSV(DataChunk& insertChunk, std::string& errorMessage) {
    return TryParseSimpleCSV(insertChunk, errorMessage);
}

} // namespace storage
} // namespace kuzu
