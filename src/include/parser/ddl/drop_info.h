#pragma once
#include <string>

#include "common/enums/conflict_action.h"
#include "common/enums/drop_type.h"

namespace kuzu {
namespace parser {

struct DropInfo {
    std::string name;
    common::DropType dropType;
    common::ConflictAction conflictAction;
};

} // namespace parser
} // namespace kuzu
