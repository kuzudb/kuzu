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

    inline std::shared_ptr<arrow::Table> readTuples(
        std::unique_ptr<storage::ReadFileMorsel> morsel) override {
        auto csvMorsel = reinterpret_cast<storage::ReadSerialMorsel*>(morsel.get());
        return csvMorsel->recordTable;
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ReadCSV>(
            nodeOffsetPos, dataColumnPoses, sharedState, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
