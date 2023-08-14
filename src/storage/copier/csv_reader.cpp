#include "storage/copier/csv_reader.h"
#include "common/string_utils.h"

#include <vector>

using namespace kuzu::common;

namespace kuzu {
namespace storage {

BaseCSVReader::BaseCSVReader(const std::string& filePath, common::CSVReaderConfig* csvReaderConfig,
                             catalog::TableSchema* tableSchema)
        : csvReaderConfig(csvReaderConfig), filePath(filePath), tableSchema(tableSchema){
}

BaseCSVReader::~BaseCSVReader() {
}

void BaseCSVReader::AddValue(std::string str_val, column_id_t &column, std::vector<uint64_t> &escape_positions, bool has_quotes,
                             uint64_t buffer_idx) {
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
        throw CopyException(StringUtils::string_format("Error in file {}, on line {}: expected {} values per row, but got more. ", filePath, linenr, return_types.size()));
    }

    // insert the line number into the chunk
    uint64_t row_entry = parseChunk.size();


    auto &v = parseChunk.data[column];
    auto parse_data = FlatVector::GetData<string_t>(v);
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
        parse_data[row_entry] = StringVector::AddStringOrBlob(v, string_t(new_val));
    } else {
        parse_data[row_entry] = str_val;
    }
    // move to the next column
    column++;
}

bool BaseCSVReader::AddRow(DataChunk &insert_chunk, column_id_t &column, std::string &error_message, uint64_t buffer_idx) {
    linenr++;

    if (row_empty) {
        row_empty = false;
        if (return_types.size() != 1) {
            if (mode == ParserMode::PARSING) {
                FlatVector::SetNull(parse_chunk.data[0], parse_chunk.size(), false);
            }
            column = 0;
            return false;
        }
    }

    if (column < return_types.size()) {
        throw CopyException(StringUtils::string_format("Error in file {} on line {}: expected {} values per row, but got {}", filePath, linenr, return_types.size(), column));
    }

        parse_chunk.SetCardinality(parse_chunk.size() + 1);

    if (mode == ParserMode::PARSING_HEADER) {
        return true;
    }

    if (mode == ParserMode::PARSING && parse_chunk.size() == STANDARD_VECTOR_SIZE) {
        Flush(insert_chunk, buffer_idx);
        return true;
    }

    column = 0;
    return false;
}

bool BaseCSVReader::Flush(DataChunk &insert_chunk, uint64_t buffer_idx, bool try_add_line) {
        if (parse_chunk.size() == 0) {
            return true;
        }
        bool conversion_error_ignored = false;
        // convert the columns in the parsed chunk to the types of the table
        insert_chunk.SetCardinality(parse_chunk);
        if (reader_data.column_ids.empty() && !reader_data.empty_columns) {
            throw InternalException("BaseCSVReader::Flush called on a CSV reader that was not correctly initialized. Call "
                                    "MultiFileReader::InitializeReader or InitializeProjection");
        }
        assert(reader_data.column_ids.size() == reader_data.column_mapping.size());
        for (idx_t c = 0; c < reader_data.column_ids.size(); c++) {
            auto col_idx = reader_data.column_ids[c];
            auto result_idx = reader_data.column_mapping[c];
            auto &parse_vector = parse_chunk.data[col_idx];
            auto &result_vector = insert_chunk.data[result_idx];
            auto &type = result_vector.GetType();
            if (type.id() == LogicalTypeID::STRING) {
                // target type is varchar: no need to convert
                // just test that all strings are valid utf-8 strings
                VerifyUTF8(col_idx);
                // reinterpret rather than reference so we can deal with user-defined types
                result_vector.Reinterpret(parse_vector);
            } else {
                string error_message;
                bool success;
                idx_t line_error = 0;
                bool target_type_not_varchar = false;
                if (options.has_format[LogicalTypeId::DATE] && type.id() == LogicalTypeId::DATE) {
                    // use the date format to cast the chunk
                    success = TryCastDateVector(options, parse_vector, result_vector, parse_chunk.size(), error_message,
                                                line_error);
                } else if (options.has_format[LogicalTypeId::TIMESTAMP] && type.id() == LogicalTypeId::TIMESTAMP) {
                    // use the date format to cast the chunk
                    success =
                            TryCastTimestampVector(options, parse_vector, result_vector, parse_chunk.size(), error_message);
                } else if (options.decimal_separator != "." &&
                           (type.id() == LogicalTypeId::FLOAT || type.id() == LogicalTypeId::DOUBLE)) {
                    success = TryCastFloatingVectorCommaSeparated(options, parse_vector, result_vector, parse_chunk.size(),
                                                                  error_message, type, line_error);
                } else if (options.decimal_separator != "." && type.id() == LogicalTypeId::DECIMAL) {
                    success = TryCastDecimalVectorCommaSeparated(options, parse_vector, result_vector, parse_chunk.size(),
                                                                 error_message, type);
                } else {
                    // target type is not varchar: perform a cast
                    target_type_not_varchar = true;
                    success =
                            VectorOperations::TryCast(context, parse_vector, result_vector, parse_chunk.size(), &error_message);
                }
                if (success) {
                    continue;
                }
                if (try_add_line) {
                    return false;
                }

                std::string col_name = to_string(col_idx);
                if (col_idx < names.size()) {
                    col_name = "\"" + names[col_idx] + "\"";
                }


                // The line_error must be summed with linenr (All lines emmited from this batch)
                // But subtracted from the parse_chunk
                D_ASSERT(line_error + linenr >= parse_chunk.size());
                line_error += linenr;
                line_error -= parse_chunk.size();

                auto error_line = line_error + 1;
                throw CopyException(StringUtils::string_format("{} at line {} in column {}", error_message,
                                                error_line, col_name));
            }
        }
        parse_chunk.Reset();
        return true;
    }


//void BaseCSVReader::InitParseChunk(column_id_t numCols) {
//
//    if (num_cols == parse_chunk.ColumnCount()) {
//        // Reset DataChunk
//        parse_chunk.Reset();
//    } else {
//        // Make New DataChunk
//        parse_chunk.Destroy();
//
//        // initialize the parse_chunk with a set of VARCHAR types
//        vector<LogicalType> varchar_types(num_cols, LogicalType::VARCHAR);
//        parse_chunk.Initialize(allocator, varchar_types);
//    }
//}

BufferedCSVReader::BufferedCSVReader(const std::string& filePath, common::CSVReaderConfig* csvReaderConfig,
                                     catalog::TableSchema* tableSchema)
            : BaseCSVReader(filePath, csvReaderConfig, tableSchema), bufferSize(0), position(0), start(0) {
        Initialize(tableSchema->getProperties());
}

void BufferedCSVReader::Initialize(std::vector<kuzu::catalog::Property*> properties) {
    //PrepareComplexParser();
    for (auto property : properties) {
        return_types.push_back(*property->getDataType());
    }
    ResetBuffer();
    ReadHeader(csvReaderConfig->hasHeader);
    //InitParseChunk(return_types.size());
}

void BufferedCSVReader::ResetBuffer() {
    buffer.reset();
    bufferSize = 0;
    position = 0;
    start = 0;
    cachedBuffers.clear();
}

void BufferedCSVReader::ReadHeader(bool hasHeader) {
    if (hasHeader) {
        // ignore the first line as a header line
        //InitParseChunk(return_types.size());
        // ParseCSV(ParserMode::PARSING_HEADER);
        ;
    }
}

bool BufferedCSVReader::ReadBuffer(uint64_t &start, uint64_t &line_start) {
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
    uint64_t read_count = file_handle->Read(buffer.get() + remaining, buffer_read_size);

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
    if (parseChunk.data.size() == 1) {
        // Empty lines are null data.
        return;
    }
    for (; position < bufferSize; position++) {
        if (!StringUtils::CharacterIsNewline(buffer[position])) {
            return;
        }
    }
}


bool BufferedCSVReader::TryParseSimpleCSV(DataChunk &insertChunk, std::string &errorMessage) {
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
            return true;
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
    AddValue(std::string(buffer.get() + start, position - start - offset), column, escape_positions, has_quotes);
    // increase position by 1 and move start to the new position
    offset = 0;
    has_quotes = false;
    start = ++position;
    if (position >= bufferSize && !ReadBuffer(start, line_start)) {
        // file ends right after delimiter, go to final state
        goto final_state;
    }
    goto value_start;
    add_row :
    {
        // check type of newline (\r or \n)
        bool carriage_return = buffer[position] == '\r';
        AddValue(std::string(buffer.get() + start, position - start - offset), column, escape_positions, has_quotes);
        if (!errorMessage.empty()) {
            return false;
        }
        finished_chunk = AddRow(insertChunk, column, errorMessage);
        if (!errorMessage.empty()) {
            return false;
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
                return true;
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
    throw CopyException(StringUtils::string_format("Error in file {} on line {}: unterminated quotes. ", filePath, linenr));
    unquote:
    /* state: unquote */
    // this state handles the state directly after we unquote
    // in this state we expect either another quote (entering the quoted state again, and escaping the quote)
    // or a delimiter/newline, ending the current value and moving on to the next value
    position++;
    if (position >= bufferSize && !ReadBuffer(start, line_start)) {
        // file ends right after unquote, go to final state
        offset = 1;
        goto final_state;
    }
    if (buffer[position] == csvReaderConfig->quoteChar && (!csvReaderConfig->escapeChar || csvReaderConfig->escapeChar == csvReaderConfig->quoteChar)) {
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
        return false;
    }
    handle_escape:
    /* state: handle_escape */
    // escape should be followed by a quote or another escape character
    position++;
    if (position >= bufferSize && !ReadBuffer(start, line_start)) {
        errorMessage = StringUtils::string_format(
                "Error in file {} on line {}: neither QUOTE nor ESCAPE is proceeded by ESCAPE.",
                filePath, linenr);
        return false;
    }
    if (buffer[position] != csvReaderConfig->quoteChar && buffer[position] != csvReaderConfig->escapeChar) {
        errorMessage = StringUtils::string_format(
                "Error in file {} on line {}}: neither QUOTE nor ESCAPE is proceeded by ESCAPE.",
                filePath, linenr);
        return false;
    }
    // escape was followed by quote or escape, go back to quoted state
    goto in_quotes;
    carriage_return:
    /* state: carriage_return */
    // this stage optionally skips a newline (\n) character, which allows \r\n to be interpreted as a single line
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
        return true;
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
        return true;
    }

    if (column > 0 || position > start) {
        // remaining values to be added to the chunk
        AddValue(std::string(buffer.get() + start, position - start - offset), column, escape_positions, has_quotes);
        finished_chunk = AddRow(insertChunk, column, errorMessage);
        SkipEmptyLines();
        if (!errorMessage.empty()) {
            return false;
        }
    }

    // final stage, only reached after parsing the file is finished
    // flush the parsed chunk and finalize parsing
    if (mode == ParserMode::PARSING) {
        Flush(insertChunk);
    }

    end_of_file_reached = true;
    return true;
}

void BufferedCSVReader::ParseCSV(common::DataChunk &insertChunk) {
    std::string errorMessage;
    if (!TryParseCSV(ParserMode::PARSING, insertChunk, errorMessage)) {
        throw CopyException(errorMessage);
    }
}

bool BufferedCSVReader::TryParseCSV(ParserMode parserMode, DataChunk &insertChunk, std::string &errorMessage) {
    mode = parserMode;
    return TryParseSimpleCSV(insertChunk, errorMessage);
}


} // namespace storage
} // namespace kuzu