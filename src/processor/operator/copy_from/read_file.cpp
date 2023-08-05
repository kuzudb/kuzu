#include "processor/operator/copy_from/read_file.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

bool ReadFile::getNextTuplesInternal(ExecutionContext* context) {
    auto morsel = sharedState->getMorsel();
    if (morsel == nullptr) {
        return false;
    }
    auto recordBatch = readTuples(std::move(morsel));
    for (auto i = 0u; i < dataColumnPoses.size(); i++) {
        ArrowColumnVector::setArrowColumn(
            resultSet->getValueVector(dataColumnPoses[i]).get(), recordBatch->column((int)i));
    }
    resultSet->dataChunks[0]->state->setToUnflat();
    return true;
}

} // namespace processor
} // namespace kuzu
