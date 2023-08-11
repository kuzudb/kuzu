#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/copier/read_file_state.h"

namespace kuzu {
namespace processor {

class ReadFile : public PhysicalOperator {
public:
    ReadFile(const DataPos& nodeOffsetPos, std::vector<DataPos> dataColumnPoses,
        std::shared_ptr<storage::ReadFileSharedState> sharedState,
        PhysicalOperatorType operatorType, uint32_t id, const std::string& paramsString,
        bool preservingOrder)
        : PhysicalOperator{operatorType, id, paramsString}, nodeOffsetPos{nodeOffsetPos},
          dataColumnPoses{std::move(dataColumnPoses)}, sharedState{std::move(sharedState)},
          preservingOrder{preservingOrder} {}

    inline void initGlobalStateInternal(kuzu::processor::ExecutionContext* context) override {
        sharedState->countNumRows();
    }

    inline bool isSource() const override { return true; }

protected:
    virtual std::shared_ptr<arrow::Table> readTuples(
        std::unique_ptr<storage::ReadFileMorsel> morsel) = 0;

    bool getNextTuplesInternal(ExecutionContext* context) override;

protected:
    std::shared_ptr<storage::ReadFileSharedState> sharedState;
    DataPos nodeOffsetPos;
    std::vector<DataPos> dataColumnPoses;
    bool preservingOrder;
};

} // namespace processor
} // namespace kuzu
