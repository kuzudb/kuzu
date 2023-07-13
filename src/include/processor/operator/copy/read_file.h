#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/copier/table_copy_utils.h"

namespace kuzu {
namespace processor {

class ReadFileMorsel {
public:
    static constexpr common::block_idx_t INVALID_BLOCK_IDX = UINT64_MAX;

    ReadFileMorsel(common::block_idx_t blockIdx, uint64_t numNodes, std::string filePath)
        : blockIdx{blockIdx}, numNodes{numNodes}, filePath{std::move(filePath)} {};

    virtual ~ReadFileMorsel() = default;

public:
    common::block_idx_t blockIdx;
    uint64_t numNodes;
    std::string filePath;
};

class ReadFileSharedState {
public:
    explicit ReadFileSharedState(
        std::vector<std::string> filePaths, catalog::TableSchema* tableSchema)
        : curBlockIdx{0}, filePaths{std::move(filePaths)}, curFileIdx{0}, numRows{0},
          tableSchema{tableSchema} {
        assert(tableSchema);
    }

    virtual ~ReadFileSharedState() = default;

    virtual void countNumLines() = 0;

    virtual std::unique_ptr<ReadFileMorsel> getMorsel() = 0;

    inline std::vector<std::string> getFilePaths() { return filePaths; }

public:
    uint64_t numRows;
    catalog::TableSchema* tableSchema;

protected:
    std::mutex mtx;
    std::unordered_map<std::string, storage::FileBlockInfo> fileBlockInfos;
    common::block_idx_t curBlockIdx;
    std::vector<std::string> filePaths;
    common::vector_idx_t curFileIdx;
};

class ReadFile : public PhysicalOperator {
public:
    ReadFile(std::vector<DataPos> arrowColumnPoses,
        std::shared_ptr<ReadFileSharedState> sharedState, PhysicalOperatorType operatorType,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString},
          arrowColumnPoses{std::move(arrowColumnPoses)}, sharedState{std::move(sharedState)} {}

    inline void initGlobalStateInternal(kuzu::processor::ExecutionContext* context) override {
        sharedState->countNumLines();
    }

    inline bool isSource() const override { return true; }

    bool getNextTuplesInternal(ExecutionContext* context) override;

protected:
    virtual std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<ReadFileMorsel> morsel) = 0;

protected:
    std::shared_ptr<ReadFileSharedState> sharedState;
    std::vector<DataPos> arrowColumnPoses;
};

} // namespace processor
} // namespace kuzu
