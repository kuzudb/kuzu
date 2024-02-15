#include "catalog/export_utils.h"

#include <sstream>

#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace catalog {

void CatalogExportUtils::getCypher(
    const std::vector<kuzu::catalog::Property>* properties, std::stringstream& ss) {
    for (auto& prop : *properties) {
        if (prop.getDataType()->getPhysicalType() == PhysicalTypeID::INTERNAL_ID) {
            continue;
        }
        auto propStr = prop.getDataType()->toString();
        StringUtils::replaceAll(propStr, ":", " ");
        if (propStr.find("MAP") != std::string::npos) {
            StringUtils::replaceAll(propStr, "  ", ",");
        }
        ss << prop.getName() << " " << propStr << ",";
    }
}

} // namespace catalog
} // namespace kuzu
