#pragma once

#include "common/data_chunk/sel_vector.h"

namespace kuzu {
namespace common {
class DataChunkState;
} // namespace common

namespace processor {

class SelVectorOverWriter {
public:
    SelVectorOverWriter();
    virtual ~SelVectorOverWriter() = default;

protected:
    void restoreSelVector(common::DataChunkState& dataChunkState) const;

    void saveSelVector(common::DataChunkState& dataChunkState);

private:
    virtual void resetCurrentSelVector(const common::SelectionVector& selVector);

protected:
    std::shared_ptr<common::SelectionVector> prevSelVector;
    std::shared_ptr<common::SelectionVector> currentSelVector;
};
} // namespace processor
} // namespace kuzu
