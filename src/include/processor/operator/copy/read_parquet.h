#pragma once

#include "processor/operator/copy/read_file.h"

namespace kuzu {
namespace processor {

class ReadParquet : public ReadFile {
public:
    ReadParquet(const DataPos& nodeGroupOffsetPos, std::vector<DataPos> dataColumnPoses,
        std::shared_ptr<storage::ReadFileSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : ReadFile{nodeGroupOffsetPos, std::move(dataColumnPoses), std::move(sharedState),
              PhysicalOperatorType::READ_PARQUET, id, paramsString} {}

    std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<storage::ReadFileMorsel> morsel) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ReadParquet>(
            nodeGroupOffsetPos, dataColumnPoses, sharedState, id, paramsString);
    }

private:
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::string filePath;
};

} // namespace processor
} // namespace kuzu
