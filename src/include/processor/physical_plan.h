#pragma once

#include <memory>

#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

class PhysicalPlan {

public:
    explicit PhysicalPlan(unique_ptr<PhysicalOperator> lastOperator, bool readOnly)
        : lastOperator{move(lastOperator)}, readOnly{readOnly} {}

    inline bool isReadOnly() const { return readOnly; }

    inline bool isCopyCSV() const {
        return lastOperator->getChild(0)->getOperatorType() == COPY_REL_CSV ||
               lastOperator->getChild(0)->getOperatorType() == COPY_NODE_CSV;
    }

    inline bool isDDL() const {
        return lastOperator->getChild(0)->getOperatorType() == CREATE_NODE_TABLE ||
               lastOperator->getChild(0)->getOperatorType() == CREATE_REL_TABLE ||
               lastOperator->getChild(0)->getOperatorType() == DROP_TABLE;
    }

public:
    unique_ptr<PhysicalOperator> lastOperator;
    bool readOnly;
};

class PhysicalPlanUtil {
public:
    static vector<PhysicalOperator*> collectOperators(
        PhysicalOperator* root, PhysicalOperatorType operatorType);

private:
    static void collectOperatorsRecursive(
        PhysicalOperator* op, PhysicalOperatorType operatorType, vector<PhysicalOperator*>& result);
};

} // namespace processor
} // namespace kuzu
