#pragma once

#include <cstdint>

namespace kuzu {
namespace transaction {

enum class TransactionAction : uint8_t {
    BEGIN_READ = 0,
    BEGIN_WRITE = 1,
    COMMIT = 10,
    ROLLBACK = 20,
};

}
} // namespace kuzu
