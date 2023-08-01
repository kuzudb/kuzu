#include "processor/operator/copy/read_npy.h"

#include "common/constants.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::shared_ptr<arrow::RecordBatch> ReadNPY::readTuples(std::unique_ptr<ReadFileMorsel> morsel) {
    if (morsel->filePath.empty()) {
        auto serialMorsel = reinterpret_cast<storage::ReadSerialMorsel*>(morsel.get());
        return serialMorsel->recordBatch;
    }
    return reader->readBlock(morsel->blockIdx);
}

} // namespace processor
} // namespace kuzu
