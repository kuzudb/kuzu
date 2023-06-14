#pragma once

#include "processor/operator/copy/read_file.h"

namespace kuzu {
namespace processor {

// For CSV file, we need to read in streaming mode, so we need to read one batch at a time.
class ReadCSVMorsel : public ReadFileMorsel {
public:
    ReadCSVMorsel(std::string filePath, std::shared_ptr<arrow::RecordBatch> recordBatch)
        : ReadFileMorsel{INVALID_BLOCK_IDX, UINT64_MAX, std::move(filePath)}, recordBatch{std::move(
                                                                                  recordBatch)} {}

    std::shared_ptr<arrow::RecordBatch> recordBatch;
};

class ReadCSVSharedState : public ReadFileSharedState {
public:
    ReadCSVSharedState(common::CSVReaderConfig csvReaderConfig, std::vector<std::string> filePaths)
        : ReadFileSharedState{std::move(filePaths)}, csvReaderConfig{csvReaderConfig} {
    }

private:
    void countNumLines() override;

    std::unique_ptr<ReadFileMorsel> getMorsel() override;

private:
    common::CSVReaderConfig csvReaderConfig;
    std::shared_ptr<arrow::csv::StreamingReader> reader;
};

class ReadCSV : public ReadFile {
public:
    ReadCSV(std::vector<DataPos> arrowColumnPoses, std::shared_ptr<ReadFileSharedState> sharedState,
        uint32_t id, const std::string& paramsString)
        : ReadFile{std::move(arrowColumnPoses), std::move(sharedState),
              PhysicalOperatorType::READ_CSV, id, paramsString} {}

    inline std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<ReadFileMorsel> morsel) override {
        auto csvMorsel = reinterpret_cast<ReadCSVMorsel*>(morsel.get());
        return csvMorsel->recordBatch;
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ReadCSV>(arrowColumnPoses, sharedState, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
