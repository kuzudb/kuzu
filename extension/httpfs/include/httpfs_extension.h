#pragma once

#include "main/database.h"
#include "main/extension.h"

namespace kuzu {
namespace httpfs {

class HttpfsExtension : public main::Extension {
public:
    static void load(main::Database& db);
};

} // namespace httpfs
} // namespace kuzu
