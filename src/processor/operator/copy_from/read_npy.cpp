#include "processor/operator/copy_from/read_npy.h"

#include "common/constants.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

std::shared_ptr<arrow::Table> ReadNPY::readTuples(std::unique_ptr<ReadFileMorsel> morsel) {
    if (preservingOrder) {
        auto serialMorsel = reinterpret_cast<storage::ReadSerialMorsel*>(morsel.get());
        return serialMorsel->recordTable;
    }
    return arrow::Table::FromRecordBatches({reader->readBlock(morsel->blockIdx)}).ValueOrDie();
}

} // namespace processor
} // namespace kuzu
