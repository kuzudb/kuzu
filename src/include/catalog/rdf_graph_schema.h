#pragma once

#include "common/constants.h"
#include "common/string_utils.h"
#include "common/types/types_include.h"
#include "property.h"

namespace kuzu {
namespace catalog {

struct RDFGraphSchema {
public:
    RDFGraphSchema()
        : RDFGraphSchema{"", common::INVALID_RDF_GRAPH_ID, common::INVALID_TABLE_ID,
              common::INVALID_TABLE_ID} {}
    RDFGraphSchema(std::string rdfGraphName, common::rdf_graph_id_t rdfGraphID,
        common::table_id_t resourcesNodeTableID, common::table_id_t triplesRelTableID)
        : rdfGraphName{std::move(rdfGraphName)}, rdfGraphID{rdfGraphID},
          resourcesNodeTableID{resourcesNodeTableID}, triplesRelTableID{triplesRelTableID} {}

    inline std::string getRDFGraphName() const { return rdfGraphName; }

    inline common::rdf_graph_id_t getRDFGraphID() const { return rdfGraphID; }

    inline common::table_id_t getResourcesNodeTableID() const { return resourcesNodeTableID; }

    inline common::table_id_t getTriplesRelTableID() const { return triplesRelTableID; }

    static bool isReservedPropertyName(const std::string& propertyName) {
        return propertyName == common::InternalKeyword::RDF_PREDICATE_IRI_OFFSET_PROPERTY_NAME;
    }

    void serialize(common::FileInfo* fileInfo, uint64_t& offset);
    static std::unique_ptr<RDFGraphSchema> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

    inline std::unique_ptr<RDFGraphSchema> copy() const {
        return std::make_unique<RDFGraphSchema>(
            rdfGraphName, rdfGraphID, resourcesNodeTableID, triplesRelTableID);
    }

private:
    std::string rdfGraphName;
    common::rdf_graph_id_t rdfGraphID;
    common::table_id_t resourcesNodeTableID;
    common::table_id_t triplesRelTableID;
};

} // namespace catalog
} // namespace kuzu
