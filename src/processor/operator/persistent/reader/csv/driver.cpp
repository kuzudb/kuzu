#include "processor/operator/persistent/reader/csv/driver.h"

#include "catalog/catalog.h"
#include "common/exception/copy.h"
#include "common/string_format.h"
#include "function/cast/functions/cast_from_string_functions.h"
#include "processor/operator/persistent/reader/csv/parallel_csv_reader.h"
#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

ParsingDriver::ParsingDriver(common::DataChunk& chunk) : chunk(chunk), rowEmpty(false) {}

bool ParsingDriver::done(uint64_t rowNum) {
    return rowNum >= DEFAULT_VECTOR_CAPACITY || doneEarly();
}

void ParsingDriver::addValue(uint64_t rowNum, common::column_id_t columnIdx,
    std::string_view value) {
    uint64_t length = value.length();
    if (length == 0 && columnIdx == 0) {
        rowEmpty = true;
    } else {
        rowEmpty = false;
    }
    BaseCSVReader* reader = getReader();
    if (columnIdx == reader->numColumns && length == 0) {
        // skip a single trailing delimiter in last columnIdx
        return;
    }
    if (columnIdx >= reader->numColumns) {
        throw CopyException(
            stringFormat("Error in file {}, on line {}: expected {} values per row, but got more.",
                reader->fileInfo->path, reader->getLineNumber(), reader->numColumns));
    }
    try {
        function::CastString::copyStringToVector(chunk.getValueVector(columnIdx).get(), rowNum,
            value, &reader->option);
    } catch (ConversionException& e) {
        throw CopyException(stringFormat("Error in file {} on line {}: {}", reader->fileInfo->path,
            reader->getLineNumber(), e.what()));
    }
}

bool ParsingDriver::addRow(uint64_t /*rowNum*/, common::column_id_t columnCount) {
    BaseCSVReader* reader = getReader();
    if (rowEmpty) {
        rowEmpty = false;
        if (reader->numColumns != 1) {
            return false;
        }
        // Otherwise, treat it as null.
    }
    if (columnCount < reader->numColumns) {
        // Column number mismatch.
        throw CopyException(
            stringFormat("Error in file {} on line {}: expected {} values per row, but got {}",
                reader->fileInfo->path, reader->getLineNumber(), reader->numColumns, columnCount));
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

bool SniffCSVNameAndTypeDriver::done(uint64_t rowNum) const {
    return (csvOptions.hasHeader ? 1 : 0) + csvOptions.sampleSize <= rowNum;
}

void SniffCSVNameAndTypeDriver::addValue(uint64_t rowNum, common::column_id_t columnIdx,
    std::string_view value) {
    if (columns.size() < columnIdx + 1 && csvOptions.hasHeader && rowNum > 0) {
        throw CopyException(
            stringFormat("Error in file {}, on line {}: expected {} values per row, but got more.",
                reader->fileInfo->path, reader->getLineNumber(), columns.size()));
    }
    while (columns.size() < columnIdx + 1) {
        columns.emplace_back(stringFormat("column{}", columns.size()), LogicalType::ANY());
        sniffType.push_back(true);
    }
    if (rowNum == 0 && csvOptions.hasHeader) {
        // reading the header
        std::string columnName(value);
        LogicalType columnType(LogicalTypeID::ANY);
        auto it = value.rfind(':');
        if (it != std::string_view::npos) {
            try {
                columnType = context->getCatalog()->getType(context->getTx(),
                    std::string(value.substr(it + 1)));
                columnName = std::string(value.substr(0, it));
                sniffType[columnIdx] = false;
            } catch (const Exception&) { // NOLINT(bugprone-empty-catch):
                                         // This is how we check for a suitable
                                         // datatype name.
                // Didn't parse, just use the whole name.
            }
        }
        columns[columnIdx].first = columnName;
        columns[columnIdx].second = std::move(columnType);
    } else if (sniffType[columnIdx]) {
        // reading the body
        LogicalType combinedType;
        columns[columnIdx].second = LogicalTypeUtils::combineTypes(columns[columnIdx].second,
            function::inferMinimalTypeFromString(std::string(value)));
        if (columns[columnIdx].second.getLogicalTypeID() == LogicalTypeID::STRING) {
            sniffType[columnIdx] = false;
        }
    }
}

bool SniffCSVNameAndTypeDriver::addRow(uint64_t, common::column_id_t) {
    return true;
}

} // namespace processor
} // namespace kuzu
