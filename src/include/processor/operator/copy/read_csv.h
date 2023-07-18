#pragma once

#include "processor/operator/copy/read_file.h"

namespace kuzu {
namespace processor {

// For CSV file, we need to read in streaming mode, so we need to read one batch at a time.
class ReadCSVMorsel : public ReadFileMorsel {
public:
    ReadCSVMorsel(common::row_idx_t rowIdx, std::string filePath, common::row_idx_t rowIdxInFile,
        std::shared_ptr<arrow::RecordBatch> recordBatch)
        : ReadFileMorsel{rowIdx, common::INVALID_BLOCK_IDX, common::INVALID_ROW_IDX,
              std::move(filePath), rowIdxInFile},
          recordBatch{std::move(recordBatch)} {}

    std::shared_ptr<arrow::RecordBatch> recordBatch;
};

class ReadCSVSharedState : public ReadFileSharedState {
public:
    ReadCSVSharedState(common::CSVReaderConfig csvReaderConfig, std::vector<std::string> filePaths,
        catalog::TableSchema* tableSchema)
        : ReadFileSharedState{std::move(filePaths), tableSchema}, csvReaderConfig{csvReaderConfig} {
    }

private:
    void countNumRows() override;

    std::unique_ptr<ReadFileMorsel> getMorsel() override;

private:
    common::CSVReaderConfig csvReaderConfig;
    std::shared_ptr<arrow::csv::StreamingReader> reader;
};

class ReadCSV : public ReadFile {
public:
    ReadCSV(const DataPos& rowIdxVectorPos, const DataPos& filePathVectorPos,
        std::vector<DataPos> arrowColumnPoses, std::shared_ptr<ReadFileSharedState> sharedState,
        uint32_t id, const std::string& paramsString)
        : ReadFile{rowIdxVectorPos, filePathVectorPos, std::move(arrowColumnPoses),
              std::move(sharedState), PhysicalOperatorType::READ_CSV, id, paramsString} {}

    inline std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<ReadFileMorsel> morsel) override {
        auto csvMorsel = reinterpret_cast<ReadCSVMorsel*>(morsel.get());
        return csvMorsel->recordBatch;
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ReadCSV>(
            rowIdxVectorPos, filePathVectorPos, arrowColumnPoses, sharedState, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
