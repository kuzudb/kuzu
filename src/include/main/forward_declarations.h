#pragma once
#include "json_fwd.hpp"

namespace kuzu {
namespace binder {
class Expression;
class BoundStatementResult;
class PropertyExpression;
} // namespace binder

namespace catalog {
class Catalog;
}

namespace planner {
class LogicalPlan;
}

namespace processor {
class PhysicalOperator;
class PhysicalPlan;
class PhysicalOperatorUtils;
class FactorizedTable;
class FlatTupleIterator;

class QueryProcessor;
} // namespace processor

namespace storage {
class StorageManager;
class MemoryManager;
class BufferManager;
class WAL;
} // namespace storage

namespace testing {
class TestHelper;
class BaseGraphTest;
class ApiTest;
} // namespace testing

namespace transaction {
class TinySnbDDLTest;
class TinySnbCopyCSVTransactionTest;
class Transaction;
class TransactionManager;
enum TransactionType : uint8_t;
} // namespace transaction

using expression_vector = std::vector<std::shared_ptr<kuzu::binder::Expression>>;
using expression_pair =
    std::pair<std::shared_ptr<kuzu::binder::Expression>, std::shared_ptr<kuzu::binder::Expression>>;
} // namespace kuzu

namespace spdlog {
class logger;
namespace level {
enum level_enum : int;
};
} // namespace spdlog