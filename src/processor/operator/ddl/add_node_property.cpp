#include "processor/operator/ddl/add_node_property.h"

using namespace kuzu::storage;

namespace kuzu {
namespace processor {

// TODO(Guodong): Remove this class.
void AddNodeProperty::executeDDLInternal(ExecutionContext* context) {
    AddProperty::executeDDLInternal(context);
}

} // namespace processor
} // namespace kuzu
