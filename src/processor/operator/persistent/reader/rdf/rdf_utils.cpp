#include "processor/operator/persistent/reader/rdf/rdf_utils.h"

#include "common/keyword/rdf_keyword.h"
#include "function/cast/functions/cast_string_non_nested_functions.h"

using namespace kuzu::common;
using namespace kuzu::common::rdf;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

common::LogicalTypeID RdfUtils::getLogicalTypeID(const std::string& type) {
    if (type.starts_with(XSD)) {
        if (type.ends_with(XSD_integer)) {
            return LogicalTypeID::INT64;
        } else if (type.ends_with(XSD_double) || type.ends_with(XSD_decimal)) {
            return LogicalTypeID::DOUBLE;
        } else if (type.ends_with(XSD_boolean)) {
            return LogicalTypeID::BOOL;
        } else if (type.ends_with(XSD_date)) {
            return LogicalTypeID::DATE;
        } else if (type.ends_with(XSD_nonNegativeInteger) || type.ends_with(XSD_positiveInteger)) {
            return LogicalTypeID::UINT64;
        } else if (type.ends_with(XSD_float)) {
            return LogicalTypeID::FLOAT;
        }
    }
    return LogicalTypeID::STRING;
}

void RdfUtils::addRdfLiteral(ValueVector* vector, uint32_t pos, const std::string& str,
    LogicalTypeID targetTypeID) {
    auto resolveAsString = true;
    switch (targetTypeID) {
    case LogicalTypeID::INT64: {
        int64_t result;
        if (function::trySimpleIntegerCast(str.data(), str.size(), result)) {
            RdfVariantVector::add(vector, pos, result);
            resolveAsString = false;
        }
    } break;
    case LogicalTypeID::UINT64: {
        uint64_t result;
        if (function::trySimpleIntegerCast(str.data(), str.size(), result)) {
            RdfVariantVector::add(vector, pos, result);
            resolveAsString = false;
        }
    } break;
    case LogicalTypeID::DOUBLE: {
        double result;
        if (function::tryDoubleCast(str.data(), str.size(), result)) {
            RdfVariantVector::add(vector, pos, result);
            resolveAsString = false;
        }
    } break;
    case LogicalTypeID::FLOAT: {
        float result;
        if (function::tryDoubleCast(str.data(), str.size(), result)) {
            RdfVariantVector::add(vector, pos, result);
            resolveAsString = false;
        }
    } break;
    case LogicalTypeID::BOOL: {
        bool result;
        if (function::tryCastToBool(str.data(), str.size(), result)) {
            RdfVariantVector::add(vector, pos, result);
            resolveAsString = false;
        }
    } break;
    case LogicalTypeID::DATE: {
        date_t result;
        uint64_t dummy = 0;
        if (Date::tryConvertDate(str.data(), str.size(), dummy, result)) {
            RdfVariantVector::add(vector, pos, result);
            resolveAsString = false;
        }
    } break;
    default:
        break;
    }
    if (resolveAsString) {
        RdfVariantVector::addString(vector, pos, str.data(), str.size());
    }
}

} // namespace processor
} // namespace kuzu
