#pragma once

#include "main/connection.h"

namespace kuzu {
namespace main {

class JOConnection : public Connection {

public:
    explicit JOConnection(Database* database) : Connection{database} {}

    std::unique_ptr<QueryResult> query(const std::string& query, const std::string& encodedJoin);
};

} // namespace main
} // namespace kuzu
