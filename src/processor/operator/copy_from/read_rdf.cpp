#include "processor/operator/copy_from/read_rdf.h"

#include "common/vector/value_vector.h"
#include "storage/copier/rdf_reader.h"

namespace kuzu {
namespace processor {

std::unique_ptr<ReadRDFMorsel> ReadRDFSharedState::getMorsel(
    const std::shared_ptr<common::ValueVector>& subjectVector,
    const std::shared_ptr<common::ValueVector>& predicateVector,
    const std::shared_ptr<common::ValueVector>& objectVector) {
    std::unique_lock lck{mtx};
    std::string filePath = rdfReader->getFilePath();

    offset_t numLines = rdfReader->read(subjectVector, predicateVector, objectVector);
    if (numLines == 0) {
        return nullptr;
    }

    auto rdfMorsel = std::make_unique<ReadRDFMorsel>(currentOffset, currentOffset + numLines);
    currentOffset += numLines;
    return rdfMorsel;
}

void ReadRDFSharedState::reset() {
    std::unique_lock lck{mtx};
    rdfReader = std::make_unique<storage::RDFReader>(rdfReader->getFilePath(), false);
    currentOffset = 0;
}

offset_t ReadRDFSharedState::getApproxNumberOfIRIs() {
    return rdfReader->getFileSize() / APPROX_IRI_BYTES;
}

bool ReadRDFFile::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    auto morsel = sharedState->getMorsel(subjectVector, predicateVector, objectVector);

    if (morsel == nullptr) {
        return false;
    }

    auto startOffset = morsel->startOffset;
    auto endOffset = morsel->endOffset;
    offsetVector->setValue(offsetVector->state->selVector->selectedPositions[0], startOffset);
    offsetVector->setValue(offsetVector->state->selVector->selectedPositions[1], endOffset);
    return true;
}

} // namespace processor
} // namespace kuzu
