#pragma once

#include "common/data_chunk/data_chunk.h"
#include "common/data_chunk/data_chunk_state.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

class SelVectorOverWriter {
public:
    explicit SelVectorOverWriter() {
        currentSelVector = make_shared<SelectionVector>(DEFAULT_VECTOR_CAPACITY);
    }

protected:
    void restoreSelVector(shared_ptr<SelectionVector>& selVector);

    void saveSelVector(shared_ptr<SelectionVector>& selVector);

private:
    virtual void resetToCurrentSelVector(shared_ptr<SelectionVector>& selVector);

protected:
    shared_ptr<SelectionVector> prevSelVector;
    shared_ptr<SelectionVector> currentSelVector;
};
} // namespace processor
} // namespace kuzu
