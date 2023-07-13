#pragma once

#include "processor/operator/copy/read_file.h"
#include "storage/copier/npy_reader.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"

namespace kuzu {
namespace processor {

class ReadNPYSharedState : public ReadFileSharedState {
public:
    ReadNPYSharedState(std::vector<std::string> filePaths, catalog::TableSchema* tableSchema)
        : ReadFileSharedState{std::move(filePaths), tableSchema} {}

    std::unique_ptr<ReadFileMorsel> getMorsel() final;

private:
    void countNumLines() final;
};

class ReadNPY : public ReadFile {
public:
    ReadNPY(std::vector<DataPos> arrowColumnPoses, std::shared_ptr<ReadFileSharedState> sharedState,
        uint32_t id, const std::string& paramsString)
        : ReadFile{std::move(arrowColumnPoses), std::move(sharedState),
              PhysicalOperatorType::READ_NPY, id, paramsString} {}

    std::shared_ptr<arrow::RecordBatch> readTuples(std::unique_ptr<ReadFileMorsel> morsel) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<ReadNPY>(arrowColumnPoses, sharedState, id, paramsString);
    }

private:
    std::unique_ptr<storage::NpyMultiFileReader> reader;
};

} // namespace processor
} // namespace kuzu
