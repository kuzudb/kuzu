#pragma once

#include "catalog/property.h"

namespace kuzu {
namespace catalog {

class CatalogExportUtils {
public:
    static void getCypher(
        const std::vector<kuzu::catalog::Property>* properties, std::stringstream& ss);
};

} // namespace catalog
} // namespace kuzu
