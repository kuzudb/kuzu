#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/copier/read_file_state.h"

namespace kuzu {
namespace processor {

class ReadFile : public PhysicalOperator {
public:
    ReadFile(std::vector<DataPos> dataColumnPoses,
        std::shared_ptr<storage::ReadFileSharedState> sharedState,
        PhysicalOperatorType operatorType, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString},
          dataColumnPoses{std::move(dataColumnPoses)}, sharedState{std::move(sharedState)} {}

    inline void initGlobalStateInternal(kuzu::processor::ExecutionContext* context) override {
        sharedState->countNumRows();
    }

    inline bool isSource() const override { return true; }

protected:
    virtual std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<storage::ReadFileMorsel> morsel) = 0;

    bool getNextTuplesInternal(ExecutionContext* context) override;

protected:
    std::shared_ptr<storage::ReadFileSharedState> sharedState;
    std::vector<DataPos> dataColumnPoses;
};

} // namespace processor
} // namespace kuzu
