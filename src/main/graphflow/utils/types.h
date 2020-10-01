#ifndef GRAPHFLOW_UTILS_TYPES_H_
#define GRAPHFLOW_UTILS_TYPES_H_

#include <cfloat>
#include <cmath>
#include <cstdint>
#include <string>

namespace graphflow {
namespace utils {

typedef uint32_t gfLabel_t;
typedef uint32_t gfNodeOffset_t;
typedef int32_t gfInt_t;
typedef double_t gfDouble_t;
typedef uint8_t gfBool_t;
typedef std::string *gfString_t;

const gfInt_t NULL_GFINT = INT32_MIN;
const gfDouble_t NULL_GFDOUBLE = DBL_MIN;
const gfString_t NULL_gfSTRING = nullptr;
const gfBool_t NULL_GFBOOL = 0;

} // namespace utils
} // namespace graphflow

#endif // GRAPHFLOW_UTILS_TYPES_H_
