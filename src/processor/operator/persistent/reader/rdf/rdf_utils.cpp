#include "processor/operator/persistent/reader/rdf/rdf_utils.h"

#include "common/keyword/rdf_keyword.h"
#include "function/cast/functions/cast_string_non_nested_functions.h"

using namespace kuzu::common;
using namespace kuzu::common::rdf;
using namespace kuzu::function;

namespace kuzu {
namespace processor {

void RdfUtils::addRdfLiteral(
    common::ValueVector* vector, uint32_t pos, const char* buf, uint32_t length) {
    auto typeVector = StructVector::getFieldVector(vector, 0).get();
    auto valVector = StructVector::getFieldVector(vector, 1).get();
    typeVector->setValue<uint8_t>(pos, static_cast<uint8_t>(LogicalTypeID::STRING));
    BlobVector::addBlob(valVector, pos, buf, length);
}

template<typename T>
static void addLiteral(ValueVector* vector, uint32_t pos, LogicalTypeID typeID, T val) {
    auto typeVector = StructVector::getFieldVector(vector, 0).get();
    auto valVector = StructVector::getFieldVector(vector, 1).get();
    typeVector->setValue<uint8_t>(pos, static_cast<uint8_t>(typeID));
    BlobVector::addBlob(valVector, pos, (uint8_t*)&val, sizeof(T));
}

void RdfUtils::addRdfLiteral(common::ValueVector* vector, uint32_t pos, const char* buf,
    uint32_t length, const char* typeBuf, uint32_t typeLength) {
    bool resolveAsString = true;
    auto type = std::string_view(typeBuf, typeLength);
    if (type.starts_with(XSD)) {
        if (type.ends_with(XSD_integer)) {
            // XSD:integer
            int64_t result;
            if (function::trySimpleIntegerCast<int64_t>(buf, length, result)) {
                addLiteral<int64_t>(vector, pos, LogicalTypeID::INT64, result);
                resolveAsString = false;
            }
        } else if (type.ends_with(XSD_double) || type.ends_with(XSD_decimal)) {
            // XSD:double or XSD:decimal
            double_t result;
            if (function::tryDoubleCast(buf, length, result)) {
                addLiteral<double_t>(vector, pos, LogicalTypeID::DOUBLE, result);
                resolveAsString = false;
            }
        } else if (type.ends_with(XSD_boolean)) {
            // XSD:bool
            bool result;
            if (function::tryCastToBool(buf, length, result)) {
                addLiteral<bool>(vector, pos, LogicalTypeID::BOOL, result);
                resolveAsString = false;
            }
        } else if (type.ends_with(XSD_date)) {
            // XSD:date
            date_t result;
            uint64_t dummy = 0;
            if (Date::tryConvertDate(buf, length, dummy, result)) {
                addLiteral<date_t>(vector, pos, LogicalTypeID::DATE, result);
                resolveAsString = false;
            }
        }
    }
    if (resolveAsString) {
        addRdfLiteral(vector, pos, buf, length);
    }
}

} // namespace processor
} // namespace kuzu
