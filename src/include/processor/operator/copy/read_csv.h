#pragma once

#include "processor/operator/copy/read_file.h"

namespace kuzu {
namespace processor {

class ReadCSV : public ReadFile {
public:
    ReadCSV(const DataPos& rowIdxVectorPos, const DataPos& filePathVectorPos,
        std::vector<DataPos> dataColumnPoses,
        std::shared_ptr<storage::ReadFileSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : ReadFile{rowIdxVectorPos, filePathVectorPos, std::move(dataColumnPoses),
              std::move(sharedState), PhysicalOperatorType::READ_CSV, id, paramsString} {}

    inline std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<storage::ReadFileMorsel> morsel) override {
        auto csvMorsel = reinterpret_cast<storage::ReadCSVMorsel*>(morsel.get());
        return csvMorsel->recordBatch;
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ReadCSV>(
            rowIdxVectorPos, filePathVectorPos, dataColumnPoses, sharedState, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
