#pragma once

#include "processor/operator/copy_from/read_file.h"

namespace kuzu {
namespace processor {

class ReadCSV : public ReadFile {
public:
    ReadCSV(const DataPos& nodeOffsetPos, std::vector<DataPos> dataColumnPoses,
        std::shared_ptr<storage::ReadFileSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : ReadFile{nodeOffsetPos, std::move(dataColumnPoses), std::move(sharedState),
              PhysicalOperatorType::READ_CSV, id, paramsString, true} {}

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ReadCSV>(
            nodeOffsetPos, dataColumnPoses, sharedState, id, paramsString);
    }

private:
    inline std::shared_ptr<common::DataChunk> readDataChunk(
        std::unique_ptr<storage::ReadFileMorsel> morsel) {
        auto csvMorsel = reinterpret_cast<storage::ReadCSVMorsel*>(morsel.get());
        return csvMorsel->recordOutput;
    }
};

} // namespace processor
} // namespace kuzu
