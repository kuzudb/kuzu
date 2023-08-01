#include "processor/operator/copy/read_npy.h"

#include "common/constants.h"
#include "storage/in_mem_storage_structure/in_mem_column_chunk.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::shared_ptr<arrow::RecordBatch> ReadNPY::readTuples(std::unique_ptr<ReadFileMorsel> morsel) {
    return reader->readBlock(morsel->blockIdx);
}

} // namespace processor
} // namespace kuzu
