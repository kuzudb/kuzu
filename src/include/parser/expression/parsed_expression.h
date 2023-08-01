#pragma once

#include <memory>
#include <string>
#include <vector>

#include "common/expression_type.h"

namespace kuzu {

namespace common {
class FileInfo;
}

namespace parser {

class ParsedExpression;
class ParsedExpressionChildrenVisitor;
using parsed_expression_vector = std::vector<std::unique_ptr<ParsedExpression>>;
using parsed_expression_pair =
    std::pair<std::unique_ptr<ParsedExpression>, std::unique_ptr<ParsedExpression>>;

class ParsedExpression {
    friend class ParsedExpressionChildrenVisitor;

public:
    ParsedExpression(
        common::ExpressionType type, std::unique_ptr<ParsedExpression> child, std::string rawName);

    ParsedExpression(common::ExpressionType type, std::unique_ptr<ParsedExpression> left,
        std::unique_ptr<ParsedExpression> right, std::string rawName);

    ParsedExpression(common::ExpressionType type, std::string rawName)
        : type{type}, rawName{std::move(rawName)} {}

    ParsedExpression(common::ExpressionType type) : type{type} {}

    ParsedExpression(common::ExpressionType type, std::string alias, std::string rawName,
        parsed_expression_vector children)
        : type{type}, alias{std::move(alias)}, rawName{std::move(rawName)}, children{std::move(
                                                                                children)} {}

    virtual ~ParsedExpression() = default;

    inline common::ExpressionType getExpressionType() const { return type; }

    inline void setAlias(std::string name) { alias = std::move(name); }

    inline bool hasAlias() const { return !alias.empty(); }

    inline std::string getAlias() const { return alias; }

    inline std::string getRawName() const { return rawName; }

    inline uint32_t getNumChildren() const { return children.size(); }
    inline ParsedExpression* getChild(uint32_t idx) const { return children[idx].get(); }

    inline std::string toString() const { return rawName; }

    virtual inline std::unique_ptr<ParsedExpression> copy() const {
        return std::make_unique<ParsedExpression>(type, alias, rawName, copyChildren());
    }

    void serialize(common::FileInfo* fileInfo, uint64_t& offset) const;

    static std::unique_ptr<ParsedExpression> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

protected:
    parsed_expression_vector copyChildren() const;

private:
    virtual inline void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) const {}

protected:
    common::ExpressionType type;
    std::string alias;
    std::string rawName;
    parsed_expression_vector children;
};

} // namespace parser
} // namespace kuzu
