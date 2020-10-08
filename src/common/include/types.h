#ifndef GRAPHFLOW_UTILS_TYPES_H_
#define GRAPHFLOW_UTILS_TYPES_H_

#include <cfloat>
#include <cmath>
#include <cstdint>
#include <string>

namespace graphflow {
namespace common {

typedef uint32_t gfLabel_t;
typedef uint64_t gfNodeOffset_t;
typedef int32_t gfInt_t;
typedef double_t gfDouble_t;
typedef uint8_t gfBool_t;
typedef std::string *gfString_t;

enum DataType { NODE, REL, INT, DOUBLE, BOOLEAN, STRING };

struct Property {
    DataType dataType{};
    std::string propertyName{};

    bool operator==(const Property &o) const {
        return dataType == o.dataType && propertyName.compare(o.propertyName) == 0;
    }
};

struct hash_Property {
    std::size_t operator()(Property const &property) const {
        return (std::hash<std::string>{}(property.propertyName) * 31) + property.dataType;
    }
};

const gfInt_t NULL_GFINT = INT32_MIN;
const gfDouble_t NULL_GFDOUBLE = DBL_MIN;
const gfString_t NULL_GFSTRING = nullptr;
const gfBool_t NULL_GFBOOL = 0;

DataType getDataType(const std::string &dataTypeString);

} // namespace common
} // namespace graphflow

#endif // GRAPHFLOW_UTILS_TYPES_H_
