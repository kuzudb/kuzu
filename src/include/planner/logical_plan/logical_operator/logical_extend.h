#pragma once

#include "base_logical_operator.h"
#include "binder/expression/node_expression.h"

namespace kuzu {
namespace planner {
using namespace kuzu::binder;

class LogicalExtend : public LogicalOperator {
public:
    LogicalExtend(shared_ptr<NodeExpression> boundNode, shared_ptr<NodeExpression> nbrNode,
        table_id_t relTableID, RelDirection direction, bool isColumn, uint8_t lowerBound,
        uint8_t upperBound, shared_ptr<LogicalOperator> child)
        : LogicalOperator{move(child)}, boundNode{move(boundNode)}, nbrNode{move(nbrNode)},
          relTableID{relTableID}, direction{direction}, isColumn{isColumn}, lowerBound{lowerBound},
          upperBound{upperBound} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_EXTEND;
    }

    string getExpressionsForPrinting() const override {
        return boundNode->getRawName() + (direction == RelDirection::FWD ? "->" : "<-") +
               nbrNode->getRawName();
    }

    inline void computeSchema(Schema& schema) {
        auto boundGroupPos = schema.getGroupPos(boundNode->getInternalIDPropertyName());
        uint32_t nbrGroupPos = 0u;
        if (isColumn) {
            nbrGroupPos = boundGroupPos;
        } else {
            assert(schema.getGroup(boundGroupPos)->getIsFlat());
            nbrGroupPos = schema.createGroup();
        }
        schema.insertToGroupAndScope(nbrNode->getInternalIDProperty(), nbrGroupPos);
    }

    inline shared_ptr<NodeExpression> getBoundNodeExpression() const { return boundNode; }
    inline shared_ptr<NodeExpression> getNbrNodeExpression() const { return nbrNode; }
    inline table_id_t getRelTableID() const { return relTableID; }
    inline RelDirection getDirection() const { return direction; }
    inline bool getIsColumn() const { return isColumn; }
    inline uint8_t getLowerBound() const { return lowerBound; }
    inline uint8_t getUpperBound() const { return upperBound; }

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalExtend>(boundNode, nbrNode, relTableID, direction, isColumn,
            lowerBound, upperBound, children[0]->copy());
    }

private:
    shared_ptr<NodeExpression> boundNode;
    shared_ptr<NodeExpression> nbrNode;
    table_id_t relTableID;
    RelDirection direction;
    bool isColumn;
    uint8_t lowerBound;
    uint8_t upperBound;
};

} // namespace planner
} // namespace kuzu
