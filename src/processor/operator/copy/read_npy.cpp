#include "processor/operator/copy/read_npy.h"

#include "common/constants.h"
#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::shared_ptr<arrow::RecordBatch> ReadNPY::readTuples(std::unique_ptr<ReadFileMorsel> morsel) {
    if (!reader) {
        reader = std::make_unique<NpyMultiFileReader>(sharedState->filePaths);
    }
    auto batch = reader->readBlock(morsel->blockIdx);
    return batch;
}

} // namespace processor
} // namespace kuzu
