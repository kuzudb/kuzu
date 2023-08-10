#pragma once

#include "processor/operator/copy_from/read_file.h"

namespace kuzu {
namespace processor {

class ReadParquet : public ReadFile {
public:
    ReadParquet(const DataPos& nodeOffsetPos, std::vector<DataPos> dataColumnPoses,
        std::shared_ptr<storage::ReadFileSharedState> sharedState, uint32_t id,
        const std::string& paramsString, bool preservingOrder)
        : ReadFile{nodeOffsetPos, std::move(dataColumnPoses), std::move(sharedState),
              PhysicalOperatorType::READ_PARQUET, id, paramsString, preservingOrder} {}

    std::shared_ptr<arrow::Table> readTuples(
        std::unique_ptr<storage::ReadFileMorsel> morsel) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ReadParquet>(
            nodeOffsetPos, dataColumnPoses, sharedState, id, paramsString, preservingOrder);
    }

private:
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::string filePath;
};

} // namespace processor
} // namespace kuzu
