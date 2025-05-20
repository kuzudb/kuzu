#include "processor/operator/sink.h"

namespace kuzu {
namespace processor {

std::unique_ptr<ResultSet> Sink::getResultSet(storage::MemoryManager* memoryManager) {
    if (resultSetDescriptor == nullptr) {
        // Some pipeline does not need a resultSet, e.g. OrderByMerge
        return std::unique_ptr<ResultSet>();
    }
    return std::make_unique<ResultSet>(resultSetDescriptor.get(), memoryManager);
}

} // namespace processor
} // namespace kuzu
