#pragma once

#include "src/binder/expression/include/expression.h"

namespace graphflow {
namespace binder {

class PropertyUpdateInfo {
public:
    PropertyUpdateInfo(shared_ptr<Expression> property, shared_ptr<Expression> target)
        : property{move(property)}, target{move(target)} {
        assert(this->property->expressionType == PROPERTY &&
               this->property->dataType == this->target->dataType);
    }
    PropertyUpdateInfo(const PropertyUpdateInfo& other)
        : PropertyUpdateInfo{other.property, other.target} {}

    inline shared_ptr<Expression> getProperty() const { return property; }
    inline shared_ptr<Expression> getTarget() const { return target; }

    inline expression_vector getPropertiesToRead() const {
        return target->getSubPropertyExpressions();
    }

private:
    shared_ptr<Expression> property;
    shared_ptr<Expression> target;
};

class NodeUpdateInfo {
public:
    explicit NodeUpdateInfo(shared_ptr<Expression> node) : node{move(node)} {
        assert(this->node->expressionType == VARIABLE && this->node->dataType.typeID == NODE);
    }
    NodeUpdateInfo(const NodeUpdateInfo& other) : node{other.node} {
        for (auto& propertyUpdateInfo : other.propertyUpdateInfos) {
            propertyUpdateInfos.push_back(make_unique<PropertyUpdateInfo>(*propertyUpdateInfo));
        }
    }

    inline void addPropertyUpdateInfo(unique_ptr<PropertyUpdateInfo> propertyUpdateInfo) {
        propertyUpdateInfos.push_back(move(propertyUpdateInfo));
    }

    inline expression_vector getPropertiesToRead() const {
        expression_vector result;
        for (auto& propertyUpdateInfo : propertyUpdateInfos) {
            for (auto& property : propertyUpdateInfo->getPropertiesToRead()) {
                result.push_back(property);
            }
        }
        return result;
    }

private:
    shared_ptr<Expression> node;
    vector<unique_ptr<PropertyUpdateInfo>> propertyUpdateInfos;
};

} // namespace binder
} // namespace graphflow
