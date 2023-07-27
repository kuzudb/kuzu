#pragma once

#include "processor/operator/copy/read_file.h"
#include "storage/copier/npy_reader.h"
#include "storage/in_mem_storage_structure/in_mem_column.h"

namespace kuzu {
namespace processor {

class ReadNPY : public ReadFile {
public:
    ReadNPY(const DataPos& rowIdxVectorPos, std::vector<DataPos> arrowColumnPoses,
        std::shared_ptr<storage::ReadFileSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : ReadFile{rowIdxVectorPos, std::move(arrowColumnPoses), std::move(sharedState),
              PhysicalOperatorType::READ_NPY, id, paramsString} {}

    std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<storage::ReadFileMorsel> morsel) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<ReadNPY>(
            rowIdxVectorPos, arrowColumnPoses, sharedState, id, paramsString);
    }

private:
    std::unique_ptr<storage::NpyMultiFileReader> reader;
};

} // namespace processor
} // namespace kuzu
