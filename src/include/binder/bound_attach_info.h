#pragma once

#include "common/case_insensitive_map.h"
#include "common/types/value/value.h"

namespace kuzu {
namespace binder {

struct KUZU_API AttachOption {
    common::case_insensitive_map_t<common::Value> options;
};

struct KUZU_API AttachInfo {
    AttachInfo(std::string dbPath, std::string dbAlias, std::string dbType, AttachOption options)
        : dbPath{std::move(dbPath)}, dbAlias{std::move(dbAlias)}, dbType{std::move(dbType)},
          options{std::move(options)} {}

    std::string dbPath, dbAlias, dbType;
    AttachOption options;
};

} // namespace binder
} // namespace kuzu
