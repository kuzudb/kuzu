#pragma once

#include "processor/operator/copy/read_file.h"

namespace kuzu {
namespace processor {

// For CSV file, we need to read in streaming mode, so we need to read one batch at a time.
class ReadCSVMorsel : public ReadFileMorsel {
public:
    ReadCSVMorsel(common::offset_t startOffset, std::string filePath,
        std::shared_ptr<arrow::RecordBatch> recordBatch)
        : ReadFileMorsel{startOffset, BLOCK_IDX_INVALID, UINT64_MAX, std::move(filePath)},
          recordBatch{std::move(recordBatch)} {}

    std::shared_ptr<arrow::RecordBatch> recordBatch;
};

class ReadCSVSharedState : public ReadFileSharedState {
public:
    ReadCSVSharedState(common::CSVReaderConfig csvReaderConfig,
        catalog::NodeTableSchema* tableSchema, std::vector<std::string> filePaths)
        : ReadFileSharedState{std::move(filePaths)}, csvReaderConfig{std::move(csvReaderConfig)},
          tableSchema{tableSchema} {}

private:
    void countNumLines() override;

    std::unique_ptr<ReadFileMorsel> getMorsel() override;

private:
    common::CSVReaderConfig csvReaderConfig;
    catalog::NodeTableSchema* tableSchema;
    std::shared_ptr<arrow::csv::StreamingReader> reader;
};

class ReadCSV : public ReadFile {
public:
    ReadCSV(std::vector<DataPos> arrowColumnPoses, DataPos offsetVectorPos,
        std::shared_ptr<ReadFileSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : ReadFile{std::move(arrowColumnPoses), std::move(offsetVectorPos), std::move(sharedState),
              PhysicalOperatorType::READ_CSV, id, paramsString} {}

    inline std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<ReadFileMorsel> morsel) override {
        auto csvMorsel = reinterpret_cast<ReadCSVMorsel*>(morsel.get());
        return csvMorsel->recordBatch;
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ReadCSV>(
            arrowColumnPoses, offsetVectorPos, sharedState, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
