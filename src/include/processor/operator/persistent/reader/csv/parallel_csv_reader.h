#pragma once

#include "base_csv_reader.h"

namespace kuzu {
namespace processor {

//! ParallelCSVReader is a class that reads values from a stream in parallel.
class ParallelCSVReader final : public BaseCSVReader {
    friend class ParallelParsingDriver;

public:
    ParallelCSVReader(const std::string& filePath, const common::ReaderConfig& readerConfig);

    bool hasMoreToRead() const;
    uint64_t parseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk) override;
    uint64_t continueBlock(common::DataChunk& resultChunk);

protected:
    void handleQuotedNewline() override;

private:
    bool finishedBlock() const;
    void seekToBlockStart();
};

} // namespace processor
} // namespace kuzu
