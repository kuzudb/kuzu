#include "processor/operator/sink.h"

#include "processor/result/factorized_table_util.h"

namespace kuzu {
namespace processor {

std::unique_ptr<ResultSet> Sink::getResultSet(storage::MemoryManager* memoryManager) {
    if (resultSetDescriptor == nullptr) {
        // Some pipeline does not need a resultSet, e.g. OrderByMerge
        return std::unique_ptr<ResultSet>();
    }
    return std::make_unique<ResultSet>(resultSetDescriptor.get(), memoryManager);
}

void SimpleSink::appendMessage(const std::string& msg, storage::MemoryManager* memoryManager) {
    FactorizedTableUtils::appendStringToTable(messageTable.get(), msg, memoryManager);
}

} // namespace processor
} // namespace kuzu
