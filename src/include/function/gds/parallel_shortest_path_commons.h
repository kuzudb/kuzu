#pragma once

#include "gds.h"
using namespace kuzu::binder;
using namespace kuzu::common;

namespace kuzu {
namespace function {

struct ParallelShortestPathBindData final : public GDSBindData {
    uint8_t upperBound;

    ParallelShortestPathBindData(std::shared_ptr<Expression> nodeInput, uint8_t upperBound)
        : GDSBindData{std::move(nodeInput)}, upperBound{upperBound} {}
    ParallelShortestPathBindData(const ParallelShortestPathBindData& other)
        : GDSBindData{other}, upperBound{other.upperBound} {}

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<ParallelShortestPathBindData>(*this);
    }
};

class ParallelShortestPathLocalState : public GDSLocalState {
public:
    explicit ParallelShortestPathLocalState(main::ClientContext* clientContext) {
        auto mm = clientContext->getMemoryManager();
        srcNodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
        dstNodeIDVector = std::make_unique<ValueVector>(*LogicalType::INTERNAL_ID(), mm);
        lengthVector = std::make_unique<ValueVector>(*LogicalType::INT64(), mm);
        srcNodeIDVector->state = DataChunkState::getSingleValueDataChunkState();
        dstNodeIDVector->state = std::make_shared<DataChunkState>();
        lengthVector->state = dstNodeIDVector->state;
        outputVectors.push_back(srcNodeIDVector.get());
        outputVectors.push_back(dstNodeIDVector.get());
        outputVectors.push_back(lengthVector.get());
        nbrScanState = std::make_unique<graph::NbrScanState>(mm);
    }

public:
    std::unique_ptr<ValueVector> srcNodeIDVector;
    std::unique_ptr<ValueVector> dstNodeIDVector;
    std::unique_ptr<ValueVector> lengthVector;
};

}
}
