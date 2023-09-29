#pragma once

#include "base_csv_reader.h"

namespace kuzu {
namespace processor {

//! ParallelCSVReader is a class that reads values from a stream in parallel.
class ParallelCSVReader final : public BaseCSVReader {
public:
    static const uint64_t PARALLEL_BLOCK_SIZE;

    ParallelCSVReader(const std::string& filePath, const common::ReaderConfig& readerConfig);

    bool hasMoreToRead() const;
    uint64_t continueBlock(common::DataChunk& resultChunk);

protected:
    void parseBlockHook() override;
    void handleQuotedNewline() override;
    bool finishedBlockDetail() const override;
};

} // namespace processor
} // namespace kuzu
