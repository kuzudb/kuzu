#pragma once

#include "binder/ddl/property_definition.h"
#include "common/case_insensitive_map.h"

namespace kuzu {
namespace catalog {

class PropertyDefinitionCollection {
public:
    PropertyDefinitionCollection() : nextColumnID{0} {}
    explicit PropertyDefinitionCollection(common::column_id_t nextColumnID)
        : nextColumnID{nextColumnID} {}
    EXPLICIT_COPY_DEFAULT_MOVE(PropertyDefinitionCollection);

    common::idx_t size() const { return definitions.size(); }

    bool contains(const std::string& name) const { return nameToPropertyIdxMap.contains(name); }

    const std::vector<binder::PropertyDefinition>& getDefinitions() const { return definitions; }
    const binder::PropertyDefinition& getDefinition(const std::string& name) const;
    const binder::PropertyDefinition& getDefinition(common::idx_t idx) const;
    common::column_id_t getMaxColumnID() const;
    common::column_id_t getColumnID(const std::string& name) const;
    common::column_id_t getColumnID(common::idx_t idx) const;
    common::idx_t getIdx(const std::string& name) const;
    void vacuumColumnIDs(common::column_id_t nextColumnID);

    void add(const binder::PropertyDefinition& definition);
    void drop(const std::string& name);
    void rename(const std::string& name, const std::string& newName);

    std::string toCypher() const;

    void serialize(common::Serializer& serializer) const;
    static PropertyDefinitionCollection deserialize(common::Deserializer& deserializer);

private:
    PropertyDefinitionCollection(const PropertyDefinitionCollection& other)
        : nextColumnID{other.nextColumnID}, definitions{copyVector(other.definitions)},
          columnIDs{other.columnIDs}, nameToPropertyIdxMap{other.nameToPropertyIdxMap} {}

private:
    common::column_id_t nextColumnID;
    std::vector<binder::PropertyDefinition> definitions;
    std::vector<common::column_id_t> columnIDs;
    common::case_insensitive_map_t<common::idx_t> nameToPropertyIdxMap;
};

} // namespace catalog
} // namespace kuzu
