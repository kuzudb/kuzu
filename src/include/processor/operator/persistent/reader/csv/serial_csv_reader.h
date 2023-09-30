#pragma once

#include "base_csv_reader.h"

namespace kuzu {
namespace processor {

//! Serial CSV reader is a class that reads values from a stream in a single thread.
class SerialCSVReader final : public BaseCSVReader {
public:
    SerialCSVReader(const std::string& filePath, const common::ReaderConfig& readerConfig);

    //! Sniffs CSV dialect and determines skip rows, header row, column types and column names
    std::vector<std::pair<std::string, common::LogicalType>> sniffCSV();
    uint64_t parseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk) override;

protected:
    void handleQuotedNewline() override {}
};

} // namespace processor
} // namespace kuzu
