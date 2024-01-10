#include "processor/operator/persistent/reader/rdf/rdf_utils.h"

#include "common/keyword/rdf_keyword.h"
#include "function/cast/functions/cast_string_non_nested_functions.h"

using namespace kuzu::common;
using namespace kuzu::common::rdf;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

void RdfUtils::addRdfLiteral(common::ValueVector* vector, uint32_t pos, const char* buf,
    uint32_t length, const char* typeBuf, uint32_t typeLength) {
    bool resolveAsString = true;
    auto type = std::string_view(typeBuf, typeLength);
    if (type.starts_with(XSD)) {
        if (type.ends_with(XSD_integer)) {
            // XSD:integer
            int64_t result;
            if (function::trySimpleIntegerCast<int64_t>(buf, length, result)) {
                RdfVariantVector::add(vector, pos, result);
                resolveAsString = false;
            }
        } else if (type.ends_with(XSD_double) || type.ends_with(XSD_decimal)) {
            // XSD:double or XSD:decimal
            double_t result;
            if (function::tryDoubleCast(buf, length, result)) {
                RdfVariantVector::add(vector, pos, result);
                resolveAsString = false;
            }
        } else if (type.ends_with(XSD_boolean)) {
            // XSD:bool
            bool result;
            if (function::tryCastToBool(buf, length, result)) {
                RdfVariantVector::add(vector, pos, result);
                resolveAsString = false;
            }
        } else if (type.ends_with(XSD_date)) {
            // XSD:date
            date_t result;
            uint64_t dummy = 0;
            if (Date::tryConvertDate(buf, length, dummy, result)) {
                RdfVariantVector::add(vector, pos, result);
                resolveAsString = false;
            }
        }
    }
    if (resolveAsString) {
        RdfVariantVector::addString(vector, pos, buf, length);
    }
}

} // namespace processor
} // namespace kuzu
