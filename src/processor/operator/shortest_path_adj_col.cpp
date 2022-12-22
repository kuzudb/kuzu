#include "processor/operator/shortest_path_adj_col.h"

#include "iostream"

using namespace std;

namespace kuzu {
namespace processor {

bool ShortestPathAdjCol::getNextTuplesInternal() {
    if (!children[0]->getNextTuple()) {
        return false;
    }
    cout << ((int64_t*)(srcValueVector->getData()))[0] << endl;
    cout << ((int64_t*)(destValueVector->getData()))[0] << endl;
    return true;
}

} // namespace processor
} // namespace kuzu