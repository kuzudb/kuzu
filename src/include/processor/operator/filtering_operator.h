#pragma once

#include "common/data_chunk/sel_vector.h"

namespace kuzu {
namespace processor {

class SelVectorOverWriter {
public:
    explicit SelVectorOverWriter() {
        currentSelVector =
            std::make_shared<common::SelectionVector>(common::DEFAULT_VECTOR_CAPACITY);
    }

protected:
    void restoreSelVector(std::shared_ptr<common::SelectionVector>& selVector);

    void saveSelVector(std::shared_ptr<common::SelectionVector>& selVector);

private:
    virtual void resetToCurrentSelVector(std::shared_ptr<common::SelectionVector>& selVector);

protected:
    std::shared_ptr<common::SelectionVector> prevSelVector;
    std::shared_ptr<common::SelectionVector> currentSelVector;
};
} // namespace processor
} // namespace kuzu
