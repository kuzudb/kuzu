#include <cstdint>

namespace kuzu {

namespace testing {
class ApiTest;
class BaseGraphTest;
class TestHelper;
class TestHelper;
} // namespace testing

namespace benchmark {
class Benchmark;
} // namespace benchmark

namespace binder {
class Expression;
class BoundStatementResult;
class PropertyExpression;
} // namespace binder

namespace catalog {
class Catalog;
} // namespace catalog

namespace common {
enum class StatementType : uint8_t;
class Value;
} // namespace common

namespace storage {
class MemoryManager;
class BufferManager;
class StorageManager;
class WAL;
} // namespace storage

namespace planner {
class LogicalPlan;
} // namespace planner

namespace processor {
class QueryProcessor;
class FactorizedTable;
class FlatTupleIterator;
class PhysicalOperator;
class PhysicalPlan;
} // namespace processor

namespace transaction {
class Transaction;
enum class TransactionType : uint8_t;
class TransactionManager;
class TinySnbDDLTest;
class TinySnbCopyCSVTransactionTest;
} // namespace transaction

} // namespace kuzu

namespace spdlog {
class logger;
namespace level {
enum level_enum : int;
} // namespace level
} // namespace spdlog
