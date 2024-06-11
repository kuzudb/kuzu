#include "binder/query/reading_clause/bound_gds_call.h"

#include "function/gds_function.h"

using namespace kuzu::function;

namespace kuzu {
namespace binder {

const function::GDSAlgorithm* BoundGDSCallInfo::getGDS() const {
    return func->constPtrCast<GDSFunction>()->gds.get();
}

const function::GDSBindData* BoundGDSCallInfo::getBindData() const {
    return getGDS()->getBindData();
}

} // namespace binder
} // namespace kuzu
