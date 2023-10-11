#include "processor/operator/persistent/reader/csv/driver.h"

#include "common/exception/copy.h"
#include "common/exception/message.h"
#include "common/exception/parser.h"
#include "common/string_utils.h"
#include "common/type_utils.h"
#include "common/types/blob.h"
#include "common/types/value/value.h"
#include "function/cast/cast_utils.h"
#include "processor/operator/persistent/reader/csv/parallel_csv_reader.h"
#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"
#include "storage/store/table_copy_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

ParsingDriver::ParsingDriver(common::DataChunk& chunk) : chunk(chunk), rowEmpty(false) {}

void copyStringToVector(ValueVector* vector, uint64_t rowToAdd, std::string_view strVal,
    CSVReaderConfig csvReaderConfig);

static void skipWhitespace(const char*& input, const char* end) {
    while (input < end && isspace(*input)) {
        input++;
    }
}

bool skipToCloseQuotes(
    const char*& input, const char* end, const CSVReaderConfig& csvReaderConfig) {
    input++; // skip the first " '
    // TODO: escape char
    while (input != end) {
        if (*input == '\'') {
            return true;
        }
        input++;
    }
    return false;
}

static bool skipToClose(const char*& input, const char* end, uint64_t& lvl, char target,
    const CSVReaderConfig& csvReaderConfig) {
    input++;
    while (input != end) {
        if (*input == '\'') {
            if (!skipToCloseQuotes(input, end, csvReaderConfig)) {
                return false;
            }
        } else if (*input == '{') { // must have closing brackets fro {, ] if they are not quoted
            if (!skipToClose(input, end, lvl, '}', csvReaderConfig)) {
                return false;
            }
        } else if (*input == csvReaderConfig.listBeginChar) {
            if (!skipToClose(input, end, lvl, csvReaderConfig.listEndChar, csvReaderConfig)) {
                return false;
            }
            lvl++; // nested one more level
        } else if (*input == target) {
            if (target == ']') {
                lvl--;
            }
            return true;
        }
        input++;
    }
    return false; // no corresponding closing bracket
}

struct CountPartOperation {
    uint64_t count = 0;

    void handleValue(const char* start, const char* end, const CSVReaderConfig& config) { count++; }
};

struct SplitStringListOperation {
    SplitStringListOperation(uint64_t& offset, common::ValueVector* resultVector)
        : offset(offset), resultVector(resultVector) {}

    uint64_t& offset;
    ValueVector* resultVector;

    void handleValue(const char* start, const char* end, const CSVReaderConfig& csvReaderConfig) {
        // NULL
        auto start_copy = start;
        skipWhitespace(start, end);
        if (end - start >= 4 && (*start == 'N' || *start == 'n') &&
            (*(start + 1) == 'U' || *(start + 1) == 'u') &&
            (*(start + 2) == 'L' || *(start + 2) == 'l') &&
            (*(start + 3) == 'L' || *(start + 3) == 'l')) {
            auto null_end = start + 4;
            skipWhitespace(null_end, end);
            if (null_end == end) {
                start_copy = end;
            }
        }
        if (start == end) { // check if its empty string - NULL
            start_copy = end;
        }
        copyStringToVector(resultVector, offset,
            std::string_view{start_copy, (size_t)(end - start_copy)}, csvReaderConfig);
        offset++;
    }
};

template<typename T>
static bool splitCString(
    const char* input, uint64_t len, T& state, const CSVReaderConfig& csvReaderConfig) {
    auto end = input + len;
    uint64_t lvl = 1;
    bool seen_value = false;

    // locate [
    skipWhitespace(input, end);
    if (input == end || *input != csvReaderConfig.listBeginChar) {
        return false;
    }
    input++;

    // TODO: test if not skipping any white space works for all data type
    auto start_ptr = input;
    while (input < end) {
        auto ch = *input;
        if (ch == csvReaderConfig.listBeginChar) {
            if (!skipToClose(input, end, ++lvl, csvReaderConfig.listEndChar, csvReaderConfig)) {
                return false;
            }
        } else if (ch == '\'') {
            if (!skipToCloseQuotes(input, end, csvReaderConfig)) {
                return false;
            }
        } else if (ch == '{') {
            uint64_t struct_lvl = 0;
            skipToClose(input, end, struct_lvl, '}', csvReaderConfig);
        } else if (ch == csvReaderConfig.delimiter || ch == csvReaderConfig.listEndChar) { // split
            if (ch != csvReaderConfig.listEndChar || start_ptr < input || seen_value) {
                state.handleValue(start_ptr, input, csvReaderConfig);
                seen_value = true;
            }
            if (ch == csvReaderConfig.listEndChar) { // last ]
                lvl--;
                break;
            }
            start_ptr = ++input;
            continue;
        }
        input++;
    }
    skipWhitespace(++input, end);
    return (input == end && lvl == 0);
}

static void castStringToList(const char* input, uint64_t len, ValueVector* vector,
    uint64_t rowToAdd, const CSVReaderConfig& csvReaderConfig) {
    // calculate the number of elements in array
    CountPartOperation state;
    splitCString(input, len, state, csvReaderConfig);

    auto list_entry = ListVector::addList(vector, state.count);
    vector->setValue<list_entry_t>(rowToAdd, list_entry);
    auto listDataVector = common::ListVector::getDataVector(vector);

    SplitStringListOperation split{list_entry.offset, listDataVector};
    if (!splitCString(input, len, split, csvReaderConfig)) {
        throw common::ConversionException(
            "Cast failed. " + std::string{input, len} + " is not in " +
            common::LogicalTypeUtils::dataTypeToString(vector->dataType) + " range.");
    }
}

void copyStringToVector(ValueVector* vector, uint64_t rowToAdd, std::string_view strVal,
    CSVReaderConfig csvReaderConfig) {
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
        bool val;
        function::castStringToBool(strVal.data(), strVal.length(), val);
        vector->setValue(rowToAdd, val);
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
    case LogicalTypeID::MAP: {
        auto value = storage::TableCopyUtils::getVarListValue(
            strVal, 1, strVal.length() - 2, type, csvReaderConfig);
        vector->copyFromValue(rowToAdd, *value);
    } break;
    case LogicalTypeID::VAR_LIST: {
        castStringToList(strVal.data(), strVal.length(), vector, rowToAdd, csvReaderConfig);
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
                StructVector::getFieldVector(vector, fieldIdxAndValue.fieldIdx).get(), rowToAdd,
                fieldIdxAndValue.fieldValue, csvReaderConfig);
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
                copyStringToVector(StructVector::getFieldVector(vector, internalFieldIdx).get(),
                    rowToAdd, strVal, csvReaderConfig);
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

bool ParsingDriver::done(uint64_t rowNum) {
    return rowNum >= DEFAULT_VECTOR_CAPACITY || doneEarly();
}

void ParsingDriver::addValue(
    uint64_t rowNum, common::column_id_t columnIdx, std::string_view value) {
    uint64_t length = value.length();
    if (length == 0 && columnIdx == 0) {
        rowEmpty = true;
    } else {
        rowEmpty = false;
    }
    BaseCSVReader* reader = getReader();
    if (columnIdx == reader->expectedNumColumns && length == 0) {
        // skip a single trailing delimiter in last columnIdx
        return;
    }
    if (columnIdx >= reader->expectedNumColumns) {
        throw CopyException(StringUtils::string_format(
            "Error in file {}, on line {}: expected {} values per row, but got more.",
            reader->filePath, reader->getLineNumber(), reader->expectedNumColumns));
    }
    copyStringToVector(
        chunk.getValueVector(columnIdx).get(), rowNum, value, reader->csvReaderConfig);
}

bool ParsingDriver::addRow(uint64_t rowNum, common::column_id_t columnCount) {
    BaseCSVReader* reader = getReader();
    if (rowEmpty) {
        rowEmpty = false;
        if (reader->expectedNumColumns != 1) {
            return false;
        }
        // Otherwise, treat it as null.
    }
    if (columnCount < reader->expectedNumColumns) {
        // Column number mismatch.
        throw CopyException(StringUtils::string_format(
            "Error in file {} on line {}: expected {} values per row, but got {}", reader->filePath,
            reader->getLineNumber(), reader->expectedNumColumns, columnCount));
    }
    return true;
}

ParallelParsingDriver::ParallelParsingDriver(common::DataChunk& chunk, ParallelCSVReader* reader)
    : ParsingDriver(chunk), reader(reader) {}

bool ParallelParsingDriver::doneEarly() {
    return reader->finishedBlock();
}

BaseCSVReader* ParallelParsingDriver::getReader() {
    return reader;
}

SerialParsingDriver::SerialParsingDriver(common::DataChunk& chunk, SerialCSVReader* reader)
    : ParsingDriver(chunk), reader(reader) {}

bool SerialParsingDriver::doneEarly() {
    return false;
}

BaseCSVReader* SerialParsingDriver::getReader() {
    return reader;
}

bool SniffCSVNameAndTypeDriver::done(uint64_t) {
    return true;
}

void SniffCSVNameAndTypeDriver::addValue(uint64_t, common::column_id_t, std::string_view value) {
    std::string columnName(value);
    LogicalType columnType(LogicalTypeID::STRING);
    auto it = value.rfind(':');
    if (it != std::string_view::npos) {
        try {
            columnType = LogicalTypeUtils::dataTypeFromString(std::string(value.substr(it + 1)));
            columnName = std::string(value.substr(0, it));
        } catch (NotImplementedException) {
            // Just use the whole name.
        }
    }
    columns.emplace_back(columnName, columnType);
}

bool SniffCSVNameAndTypeDriver::addRow(uint64_t, common::column_id_t) {
    return true;
}

bool SniffCSVColumnCountDriver::done(uint64_t) {
    return true;
}

void SniffCSVColumnCountDriver::addValue(uint64_t, common::column_id_t, std::string_view value) {
    numColumns++;
}

bool SniffCSVColumnCountDriver::addRow(uint64_t, common::column_id_t) {
    return true;
}

} // namespace processor
} // namespace kuzu
