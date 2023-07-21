#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/copier/read_file_state.h"

namespace kuzu {
namespace processor {

class ReadFile : public PhysicalOperator {
public:
    ReadFile(const DataPos& rowIdxVectorPos, const DataPos& filePathVectorPos,
        std::vector<DataPos> arrowColumnPoses,
        std::shared_ptr<storage::ReadFileSharedState> sharedState,
        PhysicalOperatorType operatorType, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString}, rowIdxVectorPos{rowIdxVectorPos},
          filePathVectorPos{filePathVectorPos}, arrowColumnPoses{std::move(arrowColumnPoses)},
          sharedState{std::move(sharedState)}, rowIdxVector{nullptr}, filePathVector{nullptr} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

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
    DataPos rowIdxVectorPos;
    DataPos filePathVectorPos;
    std::vector<DataPos> arrowColumnPoses;
    common::ValueVector* rowIdxVector;
    common::ValueVector* filePathVector;
    std::vector<common::ValueVector*> arrowColumnVectors;
};

} // namespace processor
} // namespace kuzu
