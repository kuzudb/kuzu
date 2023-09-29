#pragma once

#include "base_csv_reader.h"

namespace kuzu {
namespace processor {

//! Serial CSV reader is a class that reads values from a stream in a single thread.
class SerialCSVReader final : public BaseCSVReader {
public:
    SerialCSVReader(const std::string& filePath, const common::ReaderConfig& readerConfig);

    //! Sniffs CSV dialect and determines skip rows, header row, column types and column names
    void sniffCSV();
    inline uint64_t getNumColumnsDetected() const { return numColumnsDetected; }

protected:
    void parseBlockHook() override {}
    void handleQuotedNewline() override {}
    bool finishedBlockDetail() const override;
};

} // namespace processor
} // namespace kuzu
