#include "function/gds/gds.h"

#include "binder/binder.h"
#include "processor/operator/gds_call.h"

using namespace kuzu::binder;
using namespace kuzu::main;
using namespace kuzu::graph;
using namespace kuzu::processor;

namespace kuzu {
namespace function {

void GDSAlgorithm::init(GDSCallSharedState* sharedState_, ClientContext* context) {
    sharedState = sharedState_;
    initLocalState(context);
}

} // namespace function
} // namespace kuzu
