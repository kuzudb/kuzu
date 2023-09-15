#include "processor/operator/persistent/reader/csv/serial_csv_reader.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

SerialCSVReader::SerialCSVReader(
    const std::string& filePath, const common::ReaderConfig& readerConfig)
    : BaseCSVReader{filePath, readerConfig} {}

void SerialCSVReader::sniffCSV() {
    readBOM();
    mode = ParserMode::SNIFFING_DIALECT;
    DataChunk dummyChunk(0);
    // If the file happens to be empty, there are zero columns.
    numColumnsDetected = 0;
    parseCSV(dummyChunk);
}

bool SerialCSVReader::finishedBlockDetail() const {
    // Never stop until we fill the chunk.
    return false;
}

} // namespace processor
} // namespace kuzu
