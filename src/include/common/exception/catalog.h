#pragma once

#include "exception.h"

namespace kuzu {
namespace common {

class CatalogException : public Exception {
public:
    explicit CatalogException(const std::string& msg) : Exception("Catalog exception: " + msg){};
};

} // namespace common
} // namespace kuzu
