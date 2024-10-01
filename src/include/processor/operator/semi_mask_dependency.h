#pragma once

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class SemiMaskDependency : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::SEMI_MASK_DEPENDENCY;

public:
    SemiMaskDependency(physical_op_vector_t children, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(children), id, std::move(printInfo)} {
        KU_ASSERT(this->children.size() > 1);
    }

    // This constructor is only used for cloning.
    SemiMaskDependency(std::unique_ptr<PhysicalOperator> child, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, std::move(child), id, std::move(printInfo)} {}

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override{};

    bool getNextTuplesInternal(ExecutionContext* context) override {
        return children[0]->getNextTuple(context);
    }

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<SemiMaskDependency>(children[0]->clone(), id, printInfo->copy());
    };
};

} // namespace processor
} // namespace kuzu
