#include "processor/operator/copy_from/read_npy.h"

#include "common/constants.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::shared_ptr<arrow::RecordBatch> ReadNPY::readTuples(std::unique_ptr<ReadFileMorsel> morsel) {
    return reader->readBlock(morsel->blockIdx);
}

} // namespace processor
} // namespace kuzu
