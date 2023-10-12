#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"

#include "common/string_format.h"
#include "processor/operator/persistent/reader/csv/driver.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

SerialCSVReader::SerialCSVReader(
    const std::string& filePath, const common::ReaderConfig& readerConfig)
    : BaseCSVReader{filePath, readerConfig} {}

std::vector<std::pair<std::string, LogicalType>> SerialCSVReader::sniffCSV() {
    readBOM();
    numColumnsDetected = 0;

    if (csvReaderConfig.hasHeader) {
        SniffCSVNameAndTypeDriver driver;
        parseCSV(driver);
        return driver.columns;
    } else {
        SniffCSVColumnCountDriver driver;
        parseCSV(driver);
        std::vector<std::pair<std::string, LogicalType>> columns;
        columns.reserve(driver.numColumns);
        for (uint64_t i = 0; i < driver.numColumns; i++) {
            columns.emplace_back(stringFormat("column{}", i), LogicalTypeID::STRING);
        }
        return columns;
    }
}

uint64_t SerialCSVReader::parseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk) {
    currentBlockIdx = blockIdx;
    if (blockIdx == 0) {
        handleFirstBlock();
    }
    SerialParsingDriver driver(resultChunk, this);
    return parseCSV(driver);
}

} // namespace processor
} // namespace kuzu
