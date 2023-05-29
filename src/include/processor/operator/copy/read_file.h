#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/copier/node_copy_executor.h"

namespace kuzu {
namespace processor {

class ReadFileMorsel {
public:
    static constexpr common::block_idx_t BLOCK_IDX_INVALID = UINT64_MAX;

    ReadFileMorsel(common::offset_t nodeOffset, common::block_idx_t blockIdx, uint64_t numNodes,
        std::string filePath)
        : nodeOffset{nodeOffset}, blockIdx{blockIdx}, numNodes{numNodes}, filePath{std::move(
                                                                              filePath)} {};

    virtual ~ReadFileMorsel() = default;

public:
    common::offset_t nodeOffset;
    common::block_idx_t blockIdx;
    uint64_t numNodes;
    std::string filePath;
};

class ReadFileSharedState {
public:
    explicit ReadFileSharedState(std::vector<std::string> filePaths)
        : nodeOffset{0}, curBlockIdx{0}, filePaths{std::move(filePaths)}, curFileIdx{0}, numRows{
                                                                                             0} {}

    virtual ~ReadFileSharedState() = default;

    virtual void countNumLines() = 0;

    virtual std::unique_ptr<ReadFileMorsel> getMorsel() = 0;

public:
    uint64_t numRows;

protected:
    std::mutex mtx;
    common::offset_t nodeOffset;
    std::unordered_map<std::string, storage::FileBlockInfo> fileBlockInfos;
    common::block_idx_t curBlockIdx;
    std::vector<std::string> filePaths;
    common::vector_idx_t curFileIdx;
};

class ReadFile : public PhysicalOperator {
public:
    ReadFile(std::vector<DataPos> arrowColumnPoses, DataPos offsetVectorPos,
        std::shared_ptr<ReadFileSharedState> sharedState, PhysicalOperatorType operatorType,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString}, arrowColumnPoses{std::move(
                                                                arrowColumnPoses)},
          offsetVectorPos{std::move(offsetVectorPos)}, sharedState{std::move(sharedState)} {}

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        offsetVector = resultSet->getValueVector(offsetVectorPos).get();
        for (auto& arrowColumnPos : arrowColumnPoses) {
            arrowColumnVectors.push_back(resultSet->getValueVector(arrowColumnPos).get());
        }
    }

    inline void initGlobalStateInternal(kuzu::processor::ExecutionContext* context) override {
        sharedState->countNumLines();
    }

    inline bool isSource() const override { return true; }

    virtual std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<ReadFileMorsel> morsel) = 0;

    bool getNextTuplesInternal(ExecutionContext* context) override;

protected:
    std::shared_ptr<ReadFileSharedState> sharedState;
    std::vector<DataPos> arrowColumnPoses;
    DataPos offsetVectorPos;
    common::ValueVector* offsetVector;
    std::vector<common::ValueVector*> arrowColumnVectors;
};

} // namespace processor
} // namespace kuzu
