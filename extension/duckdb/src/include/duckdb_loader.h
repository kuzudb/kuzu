#pragma once
#include "extension/extension_installer.h"

namespace kuzu {
namespace duckdb {

class DuckDBLoader {
private:
    static constexpr const char* DEPENDENCY_LIB_FILES[] = {"libduckdb.dylib"};

public:
    void load(main::ClientContext* context);
};

} // namespace duckdb
} // namespace kuzu