#include "processor/operator/persistent/reader/csv/driver.h"

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

bool ParsingDriver::addValue(uint64_t rowNum, common::column_id_t columnIdx,
    std::string_view value) {
    uint64_t length = value.length();
    if (length == 0 && columnIdx == 0) {
        rowEmpty = true;
    } else {
        rowEmpty = false;
    }
    BaseCSVReader* reader = getReader();

    if (columnIdx == reader->getNumColumns() && length == 0) {
        // skip a single trailing delimiter in last columnIdx
        return true;
    }
    if (columnIdx >= reader->getNumColumns()) {
        reader->handleCopyException(
            stringFormat("expected {} values per row, but got more.", reader->getNumColumns()));
        return false;
    }
    if (reader->skipColumn(columnIdx)) {
        return true;
    }
    try {
        function::CastString::copyStringToVector(chunk.getValueVector(columnIdx).get(), rowNum,
            value, &reader->option);
    } catch (ConversionException& e) {
        reader->handleCopyException(e.what());
        return false;
    }

    return true;
}

bool ParsingDriver::addRow(uint64_t /*rowNum*/, common::column_id_t columnCount) {
    BaseCSVReader* reader = getReader();
    if (rowEmpty) {
        rowEmpty = false;
        if (reader->getNumColumns() != 1) {
            return false;
        }
        // Otherwise, treat it as null.
    }
    if (columnCount < reader->getNumColumns()) {
        // Column number mismatch.
        reader->handleCopyException(stringFormat("expected {} values per row, but got {}.",
            reader->getNumColumns(), columnCount));
        return false;
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

SniffCSVNameAndTypeDriver::SniffCSVNameAndTypeDriver(SerialCSVReader* reader,
    const function::ScanTableFuncBindInput* bindInput)
    : reader{reader} {
    if (bindInput != nullptr) {
        for (auto i = 0u; i < bindInput->expectedColumnNames.size(); i++) {
            columns.push_back(
                {bindInput->expectedColumnNames[i], bindInput->expectedColumnTypes[i].copy()});
            sniffType.push_back(false);
        }
    }
}

bool SniffCSVNameAndTypeDriver::done(uint64_t rowNum) const {
    auto& csvOption = reader->getCSVOption();
    return (csvOption.hasHeader ? 1 : 0) + csvOption.sampleSize <= rowNum;
}

bool SniffCSVNameAndTypeDriver::addValue(uint64_t rowNum, common::column_id_t columnIdx,
    std::string_view value) {
    auto& csvOption = reader->getCSVOption();
    if (columns.size() < columnIdx + 1 && csvOption.hasHeader && rowNum > 0) {
        reader->handleCopyException("expected {} values per row, but got more.");
    }
    while (columns.size() < columnIdx + 1) {
        columns.emplace_back(stringFormat("column{}", columns.size()), LogicalType::ANY());
        sniffType.push_back(true);
    }
    if (rowNum == 0 && csvOption.hasHeader) {
        // reading the header
        std::string columnName(value);
        LogicalType columnType(LogicalTypeID::ANY);
        auto it = value.rfind(':');
        if (it != std::string_view::npos) {
            try {
                columnType = LogicalType::convertFromString(std::string(value.substr(it + 1)),
                    reader->getClientContext());
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
            function::inferMinimalTypeFromString(value));
        if (columns[columnIdx].second.getLogicalTypeID() == LogicalTypeID::STRING) {
            sniffType[columnIdx] = false;
        }
    }

    return true;
}

bool SniffCSVNameAndTypeDriver::addRow(uint64_t, common::column_id_t) {
    return true;
}

} // namespace processor
} // namespace kuzu
