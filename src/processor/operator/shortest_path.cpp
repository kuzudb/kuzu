#include "processor/operator/shortest_path.h"

#include "iostream"

using namespace std;

namespace kuzu {
namespace processor {

shared_ptr<ResultSet> ShortestPath::init(kuzu::processor::ExecutionContext* context) {
    resultSet = PhysicalOperator::init(context);
    srcValueVector =
        resultSet->dataChunks[srcDataPos.dataChunkPos]->getValueVector(srcDataPos.valueVectorPos);
    destValueVector =
        resultSet->dataChunks[destDataPos.dataChunkPos]->getValueVector(destDataPos.valueVectorPos);
    return resultSet;
}

bool ShortestPath::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    cout << ((int64_t*)(srcValueVector->getData()))[0] << endl;
    cout << ((int64_t*)(destValueVector->getData()))[0] << endl;
    return true;
}

} // namespace processor
} // namespace kuzu