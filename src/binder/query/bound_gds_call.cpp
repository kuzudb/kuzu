#include "binder/query/reading_clause/bound_gds_call.h"

using namespace kuzu::function;

namespace kuzu {
namespace binder {

const function::GDSAlgorithm* BoundGDSCallInfo::getGDS() const {
    return func.gds.get();
}

const function::GDSBindData* BoundGDSCallInfo::getBindData() const {
    return getGDS()->getBindData();
}

} // namespace binder
} // namespace kuzu
