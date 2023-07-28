#pragma once

#include "processor/operator/copy/read_file.h"
#include "storage/copier/npy_reader.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"

namespace kuzu {
namespace processor {

class ReadNPY : public ReadFile {
public:
    ReadNPY(const DataPos& nodeGroupOffsetPos, std::vector<DataPos> dataColumnPoses,
        std::shared_ptr<storage::ReadFileSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : ReadFile{nodeGroupOffsetPos, std::move(dataColumnPoses), std::move(sharedState),
              PhysicalOperatorType::READ_NPY, id, paramsString} {
        reader = std::make_unique<storage::NpyMultiFileReader>(this->sharedState->filePaths);
    }

    std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<storage::ReadFileMorsel> morsel) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<ReadNPY>(
            nodeGroupOffsetPos, dataColumnPoses, sharedState, id, paramsString);
    }

private:
    std::unique_ptr<storage::NpyMultiFileReader> reader;
};

} // namespace processor
} // namespace kuzu
