#include "processor/operator/var_length_extend/recursive_join.h"

namespace kuzu {
namespace processor {

void RecursiveJoin::initGlobalStateInternal(ExecutionContext* context) {

}

void RecursiveJoin::initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) {

}

void RecursiveJoin::executeInternal(ExecutionContext* context) {
    while (children[0]->getNextTuple(context)) {

        auto& visitedNodes = bfsMorsel->visitedNodes;
        for (auto i = 0u; i < )
    }
}

void RecursiveJoin::finalize(ExecutionContext* context) {

}

}
}