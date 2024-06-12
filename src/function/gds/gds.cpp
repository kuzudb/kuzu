#include "function/gds/gds.h"

#include "binder/binder.h"
#include "processor/operator/gds_call.h"

using namespace kuzu::binder;

namespace kuzu {
namespace function {

void GDSAlgorithm::init(processor::GDSCallSharedState* sharedState_, main::ClientContext* context) {
    sharedState = sharedState_;
    initLocalState(context);
}

} // namespace function
} // namespace kuzu
