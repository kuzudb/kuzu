#pragma once

#include "processor/operator/copy/read_file.h"
#include "storage/copier/npy_reader.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"

namespace kuzu {
namespace processor {

class ReadNPYMorsel : public ReadFileMorsel {
public:
    ReadNPYMorsel(common::offset_t nodeOffset, common::block_idx_t blockIdx, uint64_t numNodes,
        std::string filePath, common::vector_idx_t curFileIdx)
        : ReadFileMorsel{nodeOffset, blockIdx, numNodes, std::move(filePath)}, columnIdx{
                                                                                   curFileIdx} {}

    inline common::vector_idx_t getColumnIdx() const { return columnIdx; }

private:
    common::vector_idx_t columnIdx;
};

class ReadNPYSharedState : public ReadFileSharedState {
public:
    ReadNPYSharedState(catalog::NodeTableSchema* tableSchema, std::vector<std::string> filePaths)
        : ReadFileSharedState{std::move(filePaths), tableSchema} {}

    std::unique_ptr<ReadFileMorsel> getMorsel() final;

private:
    void countNumLines() final;
};

class ReadNPY : public ReadFile {
public:
    ReadNPY(std::vector<DataPos> arrowColumnPoses, const DataPos& offsetVectorPos,
        const DataPos& columnIdxPos, std::shared_ptr<ReadFileSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : ReadFile{std::move(arrowColumnPoses), offsetVectorPos, std::move(sharedState),
              PhysicalOperatorType::READ_NPY, id, paramsString},
          columnIdxPos{columnIdxPos}, columnIdxVector{nullptr} {}

    std::shared_ptr<arrow::RecordBatch> readTuples(std::unique_ptr<ReadFileMorsel> morsel) final;

    bool getNextTuplesInternal(ExecutionContext* context) final;

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<ReadNPY>(
            arrowColumnPoses, offsetVectorPos, columnIdxPos, sharedState, id, paramsString);
    }

private:
    std::unique_ptr<storage::NpyReader> reader;
    DataPos columnIdxPos;
    common::ValueVector* columnIdxVector;
};

} // namespace processor
} // namespace kuzu
