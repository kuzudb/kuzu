#pragma once

#include "processor/operator/physical_operator.h"
#include "storage/copier/table_copy_utils.h"

namespace kuzu {
namespace processor {

class ReadFileMorsel {
public:
    ReadFileMorsel(common::row_idx_t rowIdx, common::block_idx_t blockIdx,
        common::row_idx_t numRows, std::string filePath, common::row_idx_t rowIdxInFile)
        : rowIdx{rowIdx}, blockIdx{blockIdx}, numRows{numRows}, filePath{std::move(filePath)},
          rowIdxInFile{rowIdxInFile} {};

    virtual ~ReadFileMorsel() = default;

public:
    // When reading from multiple files, rowIdx is accumulated.
    common::row_idx_t rowIdx;
    common::block_idx_t blockIdx;
    common::row_idx_t numRows;
    std::string filePath;
    // Row idx in the current file. Equal to `rowIdx` when reading from only a single file.
    common::row_idx_t rowIdxInFile;
};

class ReadFileSharedState {
public:
    explicit ReadFileSharedState(
        std::vector<std::string> filePaths, catalog::TableSchema* tableSchema)
        : currRowIdx{0}, curBlockIdx{0}, filePaths{std::move(filePaths)}, curFileIdx{0},
          tableSchema{tableSchema}, numRows{0}, currRowIdxInCurrFile{1} {}

    virtual ~ReadFileSharedState() = default;

    virtual void countNumRows() = 0;

    virtual std::unique_ptr<ReadFileMorsel> getMorsel() = 0;

public:
    common::row_idx_t numRows;
    catalog::TableSchema* tableSchema;

protected:
    std::mutex mtx;
    common::row_idx_t currRowIdx;
    std::unordered_map<std::string, storage::FileBlockInfo> fileBlockInfos;
    common::block_idx_t curBlockIdx;
    std::vector<std::string> filePaths;
    common::vector_idx_t curFileIdx;
    common::row_idx_t currRowIdxInCurrFile;
};

class ReadFile : public PhysicalOperator {
public:
    ReadFile(const DataPos& rowIdxVectorPos, const DataPos& filePathVectorPos,
        std::vector<DataPos> arrowColumnPoses, std::shared_ptr<ReadFileSharedState> sharedState,
        PhysicalOperatorType operatorType, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString}, rowIdxVectorPos{rowIdxVectorPos},
          filePathVectorPos{filePathVectorPos}, arrowColumnPoses{std::move(arrowColumnPoses)},
          sharedState{std::move(sharedState)}, rowIdxVector{nullptr}, filePathVector{nullptr} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    inline void initGlobalStateInternal(kuzu::processor::ExecutionContext* context) override {
        sharedState->countNumRows();
    }

    inline bool isSource() const override { return true; }

    virtual std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<ReadFileMorsel> morsel) = 0;

    bool getNextTuplesInternal(ExecutionContext* context) override;

protected:
    std::shared_ptr<ReadFileSharedState> sharedState;
    DataPos rowIdxVectorPos;
    DataPos filePathVectorPos;
    std::vector<DataPos> arrowColumnPoses;
    common::ValueVector* rowIdxVector;
    common::ValueVector* filePathVector;
    std::vector<common::ValueVector*> arrowColumnVectors;
};

} // namespace processor
} // namespace kuzu
