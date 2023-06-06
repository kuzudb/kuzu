#pragma once

#include "processor/operator/copy/read_file.h"

namespace kuzu {
namespace processor {

class ReadParquetSharedState : public ReadFileSharedState {
public:
    explicit ReadParquetSharedState(std::vector<std::string> filePaths)
        : ReadFileSharedState{std::move(filePaths)} {}

private:
    void countNumLines() override;

    std::unique_ptr<ReadFileMorsel> getMorsel() override;
};

class ReadParquet : public ReadFile {
public:
    ReadParquet(std::vector<DataPos> arrowColumnPoses, DataPos offsetVectorPos,
        std::shared_ptr<ReadFileSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : ReadFile{std::move(arrowColumnPoses), std::move(offsetVectorPos), std::move(sharedState),
              PhysicalOperatorType::READ_PARQUET, id, paramsString} {}

    std::shared_ptr<arrow::RecordBatch> readTuples(std::unique_ptr<ReadFileMorsel> morsel) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ReadParquet>(
            arrowColumnPoses, offsetVectorPos, sharedState, id, paramsString);
    }

private:
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::string filePath;
};

} // namespace processor
} // namespace kuzu
