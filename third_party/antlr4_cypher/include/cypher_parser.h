
// Generated from Cypher.g4 by ANTLR 4.9

#pragma once


#include "antlr4-runtime.h"




class  CypherParser : public antlr4::Parser {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    T__44 = 45, T__45 = 46, T__46 = 47, CALL = 48, MACRO = 49, GLOB = 50, 
    COPY = 51, FROM = 52, NPY = 53, COLUMN = 54, NODE = 55, TABLE = 56, 
    DROP = 57, ALTER = 58, DEFAULT = 59, RENAME = 60, ADD = 61, PRIMARY = 62, 
    KEY = 63, REL = 64, TO = 65, EXPLAIN = 66, PROFILE = 67, UNION = 68, 
    ALL = 69, OPTIONAL = 70, MATCH = 71, UNWIND = 72, CREATE = 73, MERGE = 74, 
    ON = 75, SET = 76, DELETE = 77, WITH = 78, RETURN = 79, DISTINCT = 80, 
    STAR = 81, AS = 82, ORDER = 83, BY = 84, L_SKIP = 85, LIMIT = 86, ASCENDING = 87, 
    ASC = 88, DESCENDING = 89, DESC = 90, WHERE = 91, SHORTEST = 92, OR = 93, 
    XOR = 94, AND = 95, NOT = 96, INVALID_NOT_EQUAL = 97, MINUS = 98, FACTORIAL = 99, 
    STARTS = 100, ENDS = 101, CONTAINS = 102, IS = 103, NULL_ = 104, TRUE = 105, 
    FALSE = 106, EXISTS = 107, CASE = 108, ELSE = 109, END = 110, WHEN = 111, 
    THEN = 112, StringLiteral = 113, EscapedChar = 114, DecimalInteger = 115, 
    HexLetter = 116, HexDigit = 117, Digit = 118, NonZeroDigit = 119, NonZeroOctDigit = 120, 
    ZeroDigit = 121, RegularDecimalReal = 122, UnescapedSymbolicName = 123, 
    IdentifierStart = 124, IdentifierPart = 125, EscapedSymbolicName = 126, 
    SP = 127, WHITESPACE = 128, Comment = 129, Unknown = 130
  };

  enum {
    RuleOC_Cypher = 0, RuleKU_CopyFromCSV = 1, RuleKU_CopyFromNPY = 2, RuleKU_CopyTO = 3, 
    RuleKU_StandaloneCall = 4, RuleKU_CreateMacro = 5, RuleKU_PositionalArgs = 6, 
    RuleKU_DefaultArg = 7, RuleKU_FilePaths = 8, RuleKU_ParsingOptions = 9, 
    RuleKU_ParsingOption = 10, RuleKU_DDL = 11, RuleKU_CreateNode = 12, 
    RuleKU_CreateRel = 13, RuleKU_DropTable = 14, RuleKU_AlterTable = 15, 
    RuleKU_AlterOptions = 16, RuleKU_AddProperty = 17, RuleKU_DropProperty = 18, 
    RuleKU_RenameTable = 19, RuleKU_RenameProperty = 20, RuleKU_PropertyDefinitions = 21, 
    RuleKU_PropertyDefinition = 22, RuleKU_CreateNodeConstraint = 23, RuleKU_DataType = 24, 
    RuleKU_ListIdentifiers = 25, RuleKU_ListIdentifier = 26, RuleOC_AnyCypherOption = 27, 
    RuleOC_Explain = 28, RuleOC_Profile = 29, RuleOC_Statement = 30, RuleOC_Query = 31, 
    RuleOC_RegularQuery = 32, RuleOC_Union = 33, RuleOC_SingleQuery = 34, 
    RuleOC_SinglePartQuery = 35, RuleOC_MultiPartQuery = 36, RuleKU_QueryPart = 37, 
    RuleOC_UpdatingClause = 38, RuleOC_ReadingClause = 39, RuleKU_InQueryCall = 40, 
    RuleOC_Match = 41, RuleOC_Unwind = 42, RuleOC_Create = 43, RuleOC_Merge = 44, 
    RuleOC_MergeAction = 45, RuleOC_Set = 46, RuleOC_SetItem = 47, RuleOC_Delete = 48, 
    RuleOC_With = 49, RuleOC_Return = 50, RuleOC_ProjectionBody = 51, RuleOC_ProjectionItems = 52, 
    RuleOC_ProjectionItem = 53, RuleOC_Order = 54, RuleOC_Skip = 55, RuleOC_Limit = 56, 
    RuleOC_SortItem = 57, RuleOC_Where = 58, RuleOC_Pattern = 59, RuleOC_PatternPart = 60, 
    RuleOC_AnonymousPatternPart = 61, RuleOC_PatternElement = 62, RuleOC_NodePattern = 63, 
    RuleOC_PatternElementChain = 64, RuleOC_RelationshipPattern = 65, RuleOC_RelationshipDetail = 66, 
    RuleKU_Properties = 67, RuleOC_RelationshipTypes = 68, RuleOC_NodeLabels = 69, 
    RuleOC_NodeLabel = 70, RuleOC_RangeLiteral = 71, RuleOC_LabelName = 72, 
    RuleOC_RelTypeName = 73, RuleOC_Expression = 74, RuleOC_OrExpression = 75, 
    RuleOC_XorExpression = 76, RuleOC_AndExpression = 77, RuleOC_NotExpression = 78, 
    RuleOC_ComparisonExpression = 79, RuleKU_ComparisonOperator = 80, RuleKU_BitwiseOrOperatorExpression = 81, 
    RuleKU_BitwiseAndOperatorExpression = 82, RuleKU_BitShiftOperatorExpression = 83, 
    RuleKU_BitShiftOperator = 84, RuleOC_AddOrSubtractExpression = 85, RuleKU_AddOrSubtractOperator = 86, 
    RuleOC_MultiplyDivideModuloExpression = 87, RuleKU_MultiplyDivideModuloOperator = 88, 
    RuleOC_PowerOfExpression = 89, RuleOC_UnaryAddSubtractOrFactorialExpression = 90, 
    RuleOC_StringListNullOperatorExpression = 91, RuleOC_ListOperatorExpression = 92, 
    RuleKU_ListExtractOperatorExpression = 93, RuleKU_ListSliceOperatorExpression = 94, 
    RuleOC_StringOperatorExpression = 95, RuleOC_RegularExpression = 96, 
    RuleOC_NullOperatorExpression = 97, RuleOC_PropertyOrLabelsExpression = 98, 
    RuleOC_Atom = 99, RuleOC_Literal = 100, RuleOC_BooleanLiteral = 101, 
    RuleOC_ListLiteral = 102, RuleKU_StructLiteral = 103, RuleKU_StructField = 104, 
    RuleOC_ParenthesizedExpression = 105, RuleOC_FunctionInvocation = 106, 
    RuleOC_FunctionName = 107, RuleKU_FunctionParameter = 108, RuleOC_ExistentialSubquery = 109, 
    RuleOC_PropertyLookup = 110, RuleOC_CaseExpression = 111, RuleOC_CaseAlternative = 112, 
    RuleOC_Variable = 113, RuleOC_NumberLiteral = 114, RuleOC_Parameter = 115, 
    RuleOC_PropertyExpression = 116, RuleOC_PropertyKeyName = 117, RuleOC_IntegerLiteral = 118, 
    RuleOC_DoubleLiteral = 119, RuleOC_SchemaName = 120, RuleOC_SymbolicName = 121, 
    RuleOC_LeftArrowHead = 122, RuleOC_RightArrowHead = 123, RuleOC_Dash = 124
  };

  explicit CypherParser(antlr4::TokenStream *input);
  ~CypherParser();

  virtual std::string getGrammarFileName() const override;
  virtual const antlr4::atn::ATN& getATN() const override { return _atn; };
  virtual const std::vector<std::string>& getTokenNames() const override { return _tokenNames; }; // deprecated: use vocabulary instead.
  virtual const std::vector<std::string>& getRuleNames() const override;
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;


  class OC_CypherContext;
  class KU_CopyFromCSVContext;
  class KU_CopyFromNPYContext;
  class KU_CopyTOContext;
  class KU_StandaloneCallContext;
  class KU_CreateMacroContext;
  class KU_PositionalArgsContext;
  class KU_DefaultArgContext;
  class KU_FilePathsContext;
  class KU_ParsingOptionsContext;
  class KU_ParsingOptionContext;
  class KU_DDLContext;
  class KU_CreateNodeContext;
  class KU_CreateRelContext;
  class KU_DropTableContext;
  class KU_AlterTableContext;
  class KU_AlterOptionsContext;
  class KU_AddPropertyContext;
  class KU_DropPropertyContext;
  class KU_RenameTableContext;
  class KU_RenamePropertyContext;
  class KU_PropertyDefinitionsContext;
  class KU_PropertyDefinitionContext;
  class KU_CreateNodeConstraintContext;
  class KU_DataTypeContext;
  class KU_ListIdentifiersContext;
  class KU_ListIdentifierContext;
  class OC_AnyCypherOptionContext;
  class OC_ExplainContext;
  class OC_ProfileContext;
  class OC_StatementContext;
  class OC_QueryContext;
  class OC_RegularQueryContext;
  class OC_UnionContext;
  class OC_SingleQueryContext;
  class OC_SinglePartQueryContext;
  class OC_MultiPartQueryContext;
  class KU_QueryPartContext;
  class OC_UpdatingClauseContext;
  class OC_ReadingClauseContext;
  class KU_InQueryCallContext;
  class OC_MatchContext;
  class OC_UnwindContext;
  class OC_CreateContext;
  class OC_MergeContext;
  class OC_MergeActionContext;
  class OC_SetContext;
  class OC_SetItemContext;
  class OC_DeleteContext;
  class OC_WithContext;
  class OC_ReturnContext;
  class OC_ProjectionBodyContext;
  class OC_ProjectionItemsContext;
  class OC_ProjectionItemContext;
  class OC_OrderContext;
  class OC_SkipContext;
  class OC_LimitContext;
  class OC_SortItemContext;
  class OC_WhereContext;
  class OC_PatternContext;
  class OC_PatternPartContext;
  class OC_AnonymousPatternPartContext;
  class OC_PatternElementContext;
  class OC_NodePatternContext;
  class OC_PatternElementChainContext;
  class OC_RelationshipPatternContext;
  class OC_RelationshipDetailContext;
  class KU_PropertiesContext;
  class OC_RelationshipTypesContext;
  class OC_NodeLabelsContext;
  class OC_NodeLabelContext;
  class OC_RangeLiteralContext;
  class OC_LabelNameContext;
  class OC_RelTypeNameContext;
  class OC_ExpressionContext;
  class OC_OrExpressionContext;
  class OC_XorExpressionContext;
  class OC_AndExpressionContext;
  class OC_NotExpressionContext;
  class OC_ComparisonExpressionContext;
  class KU_ComparisonOperatorContext;
  class KU_BitwiseOrOperatorExpressionContext;
  class KU_BitwiseAndOperatorExpressionContext;
  class KU_BitShiftOperatorExpressionContext;
  class KU_BitShiftOperatorContext;
  class OC_AddOrSubtractExpressionContext;
  class KU_AddOrSubtractOperatorContext;
  class OC_MultiplyDivideModuloExpressionContext;
  class KU_MultiplyDivideModuloOperatorContext;
  class OC_PowerOfExpressionContext;
  class OC_UnaryAddSubtractOrFactorialExpressionContext;
  class OC_StringListNullOperatorExpressionContext;
  class OC_ListOperatorExpressionContext;
  class KU_ListExtractOperatorExpressionContext;
  class KU_ListSliceOperatorExpressionContext;
  class OC_StringOperatorExpressionContext;
  class OC_RegularExpressionContext;
  class OC_NullOperatorExpressionContext;
  class OC_PropertyOrLabelsExpressionContext;
  class OC_AtomContext;
  class OC_LiteralContext;
  class OC_BooleanLiteralContext;
  class OC_ListLiteralContext;
  class KU_StructLiteralContext;
  class KU_StructFieldContext;
  class OC_ParenthesizedExpressionContext;
  class OC_FunctionInvocationContext;
  class OC_FunctionNameContext;
  class KU_FunctionParameterContext;
  class OC_ExistentialSubqueryContext;
  class OC_PropertyLookupContext;
  class OC_CaseExpressionContext;
  class OC_CaseAlternativeContext;
  class OC_VariableContext;
  class OC_NumberLiteralContext;
  class OC_ParameterContext;
  class OC_PropertyExpressionContext;
  class OC_PropertyKeyNameContext;
  class OC_IntegerLiteralContext;
  class OC_DoubleLiteralContext;
  class OC_SchemaNameContext;
  class OC_SymbolicNameContext;
  class OC_LeftArrowHeadContext;
  class OC_RightArrowHeadContext;
  class OC_DashContext; 

  class  OC_CypherContext : public antlr4::ParserRuleContext {
  public:
    OC_CypherContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EOF();
    OC_StatementContext *oC_Statement();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_AnyCypherOptionContext *oC_AnyCypherOption();

   
  };

  OC_CypherContext* oC_Cypher();

  class  KU_CopyFromCSVContext : public antlr4::ParserRuleContext {
  public:
    KU_CopyFromCSVContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COPY();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_SchemaNameContext *oC_SchemaName();
    antlr4::tree::TerminalNode *FROM();
    KU_FilePathsContext *kU_FilePaths();
    KU_ParsingOptionsContext *kU_ParsingOptions();

   
  };

  KU_CopyFromCSVContext* kU_CopyFromCSV();

  class  KU_CopyFromNPYContext : public antlr4::ParserRuleContext {
  public:
    KU_CopyFromNPYContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COPY();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_SchemaNameContext *oC_SchemaName();
    antlr4::tree::TerminalNode *FROM();
    std::vector<antlr4::tree::TerminalNode *> StringLiteral();
    antlr4::tree::TerminalNode* StringLiteral(size_t i);
    antlr4::tree::TerminalNode *BY();
    antlr4::tree::TerminalNode *COLUMN();

   
  };

  KU_CopyFromNPYContext* kU_CopyFromNPY();

  class  KU_CopyTOContext : public antlr4::ParserRuleContext {
  public:
    KU_CopyTOContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COPY();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_QueryContext *oC_Query();
    antlr4::tree::TerminalNode *TO();
    antlr4::tree::TerminalNode *StringLiteral();

   
  };

  KU_CopyTOContext* kU_CopyTO();

  class  KU_StandaloneCallContext : public antlr4::ParserRuleContext {
  public:
    KU_StandaloneCallContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CALL();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_SymbolicNameContext *oC_SymbolicName();
    OC_LiteralContext *oC_Literal();

   
  };

  KU_StandaloneCallContext* kU_StandaloneCall();

  class  KU_CreateMacroContext : public antlr4::ParserRuleContext {
  public:
    KU_CreateMacroContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *MACRO();
    OC_FunctionNameContext *oC_FunctionName();
    antlr4::tree::TerminalNode *AS();
    OC_ExpressionContext *oC_Expression();
    KU_PositionalArgsContext *kU_PositionalArgs();
    std::vector<KU_DefaultArgContext *> kU_DefaultArg();
    KU_DefaultArgContext* kU_DefaultArg(size_t i);

   
  };

  KU_CreateMacroContext* kU_CreateMacro();

  class  KU_PositionalArgsContext : public antlr4::ParserRuleContext {
  public:
    KU_PositionalArgsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_SymbolicNameContext *> oC_SymbolicName();
    OC_SymbolicNameContext* oC_SymbolicName(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  KU_PositionalArgsContext* kU_PositionalArgs();

  class  KU_DefaultArgContext : public antlr4::ParserRuleContext {
  public:
    KU_DefaultArgContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();
    OC_LiteralContext *oC_Literal();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  KU_DefaultArgContext* kU_DefaultArg();

  class  KU_FilePathsContext : public antlr4::ParserRuleContext {
  public:
    KU_FilePathsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> StringLiteral();
    antlr4::tree::TerminalNode* StringLiteral(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *GLOB();

   
  };

  KU_FilePathsContext* kU_FilePaths();

  class  KU_ParsingOptionsContext : public antlr4::ParserRuleContext {
  public:
    KU_ParsingOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<KU_ParsingOptionContext *> kU_ParsingOption();
    KU_ParsingOptionContext* kU_ParsingOption(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  KU_ParsingOptionsContext* kU_ParsingOptions();

  class  KU_ParsingOptionContext : public antlr4::ParserRuleContext {
  public:
    KU_ParsingOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();
    OC_LiteralContext *oC_Literal();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  KU_ParsingOptionContext* kU_ParsingOption();

  class  KU_DDLContext : public antlr4::ParserRuleContext {
  public:
    KU_DDLContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    KU_CreateNodeContext *kU_CreateNode();
    KU_CreateRelContext *kU_CreateRel();
    KU_DropTableContext *kU_DropTable();
    KU_AlterTableContext *kU_AlterTable();

   
  };

  KU_DDLContext* kU_DDL();

  class  KU_CreateNodeContext : public antlr4::ParserRuleContext {
  public:
    KU_CreateNodeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *NODE();
    antlr4::tree::TerminalNode *TABLE();
    OC_SchemaNameContext *oC_SchemaName();
    KU_PropertyDefinitionsContext *kU_PropertyDefinitions();
    KU_CreateNodeConstraintContext *kU_CreateNodeConstraint();

   
  };

  KU_CreateNodeContext* kU_CreateNode();

  class  KU_CreateRelContext : public antlr4::ParserRuleContext {
  public:
    KU_CreateRelContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *REL();
    antlr4::tree::TerminalNode *TABLE();
    std::vector<OC_SchemaNameContext *> oC_SchemaName();
    OC_SchemaNameContext* oC_SchemaName(size_t i);
    antlr4::tree::TerminalNode *FROM();
    antlr4::tree::TerminalNode *TO();
    KU_PropertyDefinitionsContext *kU_PropertyDefinitions();
    OC_SymbolicNameContext *oC_SymbolicName();

   
  };

  KU_CreateRelContext* kU_CreateRel();

  class  KU_DropTableContext : public antlr4::ParserRuleContext {
  public:
    KU_DropTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *TABLE();
    OC_SchemaNameContext *oC_SchemaName();

   
  };

  KU_DropTableContext* kU_DropTable();

  class  KU_AlterTableContext : public antlr4::ParserRuleContext {
  public:
    KU_AlterTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ALTER();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *TABLE();
    OC_SchemaNameContext *oC_SchemaName();
    KU_AlterOptionsContext *kU_AlterOptions();

   
  };

  KU_AlterTableContext* kU_AlterTable();

  class  KU_AlterOptionsContext : public antlr4::ParserRuleContext {
  public:
    KU_AlterOptionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    KU_AddPropertyContext *kU_AddProperty();
    KU_DropPropertyContext *kU_DropProperty();
    KU_RenameTableContext *kU_RenameTable();
    KU_RenamePropertyContext *kU_RenameProperty();

   
  };

  KU_AlterOptionsContext* kU_AlterOptions();

  class  KU_AddPropertyContext : public antlr4::ParserRuleContext {
  public:
    KU_AddPropertyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ADD();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_PropertyKeyNameContext *oC_PropertyKeyName();
    KU_DataTypeContext *kU_DataType();
    antlr4::tree::TerminalNode *DEFAULT();
    OC_ExpressionContext *oC_Expression();

   
  };

  KU_AddPropertyContext* kU_AddProperty();

  class  KU_DropPropertyContext : public antlr4::ParserRuleContext {
  public:
    KU_DropPropertyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DROP();
    antlr4::tree::TerminalNode *SP();
    OC_PropertyKeyNameContext *oC_PropertyKeyName();

   
  };

  KU_DropPropertyContext* kU_DropProperty();

  class  KU_RenameTableContext : public antlr4::ParserRuleContext {
  public:
    KU_RenameTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RENAME();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *TO();
    OC_SchemaNameContext *oC_SchemaName();

   
  };

  KU_RenameTableContext* kU_RenameTable();

  class  KU_RenamePropertyContext : public antlr4::ParserRuleContext {
  public:
    KU_RenamePropertyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RENAME();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_PropertyKeyNameContext *> oC_PropertyKeyName();
    OC_PropertyKeyNameContext* oC_PropertyKeyName(size_t i);
    antlr4::tree::TerminalNode *TO();

   
  };

  KU_RenamePropertyContext* kU_RenameProperty();

  class  KU_PropertyDefinitionsContext : public antlr4::ParserRuleContext {
  public:
    KU_PropertyDefinitionsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<KU_PropertyDefinitionContext *> kU_PropertyDefinition();
    KU_PropertyDefinitionContext* kU_PropertyDefinition(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  KU_PropertyDefinitionsContext* kU_PropertyDefinitions();

  class  KU_PropertyDefinitionContext : public antlr4::ParserRuleContext {
  public:
    KU_PropertyDefinitionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_PropertyKeyNameContext *oC_PropertyKeyName();
    antlr4::tree::TerminalNode *SP();
    KU_DataTypeContext *kU_DataType();

   
  };

  KU_PropertyDefinitionContext* kU_PropertyDefinition();

  class  KU_CreateNodeConstraintContext : public antlr4::ParserRuleContext {
  public:
    KU_CreateNodeConstraintContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PRIMARY();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *KEY();
    OC_PropertyKeyNameContext *oC_PropertyKeyName();

   
  };

  KU_CreateNodeConstraintContext* kU_CreateNodeConstraint();

  class  KU_DataTypeContext : public antlr4::ParserRuleContext {
  public:
    KU_DataTypeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();
    KU_ListIdentifiersContext *kU_ListIdentifiers();
    antlr4::tree::TerminalNode *UNION();
    KU_PropertyDefinitionsContext *kU_PropertyDefinitions();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<KU_DataTypeContext *> kU_DataType();
    KU_DataTypeContext* kU_DataType(size_t i);

   
  };

  KU_DataTypeContext* kU_DataType();

  class  KU_ListIdentifiersContext : public antlr4::ParserRuleContext {
  public:
    KU_ListIdentifiersContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<KU_ListIdentifierContext *> kU_ListIdentifier();
    KU_ListIdentifierContext* kU_ListIdentifier(size_t i);

   
  };

  KU_ListIdentifiersContext* kU_ListIdentifiers();

  class  KU_ListIdentifierContext : public antlr4::ParserRuleContext {
  public:
    KU_ListIdentifierContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_IntegerLiteralContext *oC_IntegerLiteral();

   
  };

  KU_ListIdentifierContext* kU_ListIdentifier();

  class  OC_AnyCypherOptionContext : public antlr4::ParserRuleContext {
  public:
    OC_AnyCypherOptionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ExplainContext *oC_Explain();
    OC_ProfileContext *oC_Profile();

   
  };

  OC_AnyCypherOptionContext* oC_AnyCypherOption();

  class  OC_ExplainContext : public antlr4::ParserRuleContext {
  public:
    OC_ExplainContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EXPLAIN();

   
  };

  OC_ExplainContext* oC_Explain();

  class  OC_ProfileContext : public antlr4::ParserRuleContext {
  public:
    OC_ProfileContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *PROFILE();

   
  };

  OC_ProfileContext* oC_Profile();

  class  OC_StatementContext : public antlr4::ParserRuleContext {
  public:
    OC_StatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_QueryContext *oC_Query();
    KU_DDLContext *kU_DDL();
    KU_CopyFromNPYContext *kU_CopyFromNPY();
    KU_CopyFromCSVContext *kU_CopyFromCSV();
    KU_CopyTOContext *kU_CopyTO();
    KU_StandaloneCallContext *kU_StandaloneCall();
    KU_CreateMacroContext *kU_CreateMacro();

   
  };

  OC_StatementContext* oC_Statement();

  class  OC_QueryContext : public antlr4::ParserRuleContext {
  public:
    OC_QueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_RegularQueryContext *oC_RegularQuery();

   
  };

  OC_QueryContext* oC_Query();

  class  OC_RegularQueryContext : public antlr4::ParserRuleContext {
  public:
    OC_RegularQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SingleQueryContext *oC_SingleQuery();
    std::vector<OC_UnionContext *> oC_Union();
    OC_UnionContext* oC_Union(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_ReturnContext *> oC_Return();
    OC_ReturnContext* oC_Return(size_t i);

   
  };

  OC_RegularQueryContext* oC_RegularQuery();

  class  OC_UnionContext : public antlr4::ParserRuleContext {
  public:
    OC_UnionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNION();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *ALL();
    OC_SingleQueryContext *oC_SingleQuery();

   
  };

  OC_UnionContext* oC_Union();

  class  OC_SingleQueryContext : public antlr4::ParserRuleContext {
  public:
    OC_SingleQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SinglePartQueryContext *oC_SinglePartQuery();
    OC_MultiPartQueryContext *oC_MultiPartQuery();

   
  };

  OC_SingleQueryContext* oC_SingleQuery();

  class  OC_SinglePartQueryContext : public antlr4::ParserRuleContext {
  public:
    OC_SinglePartQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ReturnContext *oC_Return();
    std::vector<OC_ReadingClauseContext *> oC_ReadingClause();
    OC_ReadingClauseContext* oC_ReadingClause(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_UpdatingClauseContext *> oC_UpdatingClause();
    OC_UpdatingClauseContext* oC_UpdatingClause(size_t i);

   
  };

  OC_SinglePartQueryContext* oC_SinglePartQuery();

  class  OC_MultiPartQueryContext : public antlr4::ParserRuleContext {
  public:
    OC_MultiPartQueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SinglePartQueryContext *oC_SinglePartQuery();
    std::vector<KU_QueryPartContext *> kU_QueryPart();
    KU_QueryPartContext* kU_QueryPart(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_MultiPartQueryContext* oC_MultiPartQuery();

  class  KU_QueryPartContext : public antlr4::ParserRuleContext {
  public:
    KU_QueryPartContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_WithContext *oC_With();
    std::vector<OC_ReadingClauseContext *> oC_ReadingClause();
    OC_ReadingClauseContext* oC_ReadingClause(size_t i);
    std::vector<OC_UpdatingClauseContext *> oC_UpdatingClause();
    OC_UpdatingClauseContext* oC_UpdatingClause(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  KU_QueryPartContext* kU_QueryPart();

  class  OC_UpdatingClauseContext : public antlr4::ParserRuleContext {
  public:
    OC_UpdatingClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_CreateContext *oC_Create();
    OC_MergeContext *oC_Merge();
    OC_SetContext *oC_Set();
    OC_DeleteContext *oC_Delete();

   
  };

  OC_UpdatingClauseContext* oC_UpdatingClause();

  class  OC_ReadingClauseContext : public antlr4::ParserRuleContext {
  public:
    OC_ReadingClauseContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_MatchContext *oC_Match();
    OC_UnwindContext *oC_Unwind();
    KU_InQueryCallContext *kU_InQueryCall();

   
  };

  OC_ReadingClauseContext* oC_ReadingClause();

  class  KU_InQueryCallContext : public antlr4::ParserRuleContext {
  public:
    KU_InQueryCallContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CALL();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_FunctionNameContext *oC_FunctionName();
    std::vector<OC_LiteralContext *> oC_Literal();
    OC_LiteralContext* oC_Literal(size_t i);

   
  };

  KU_InQueryCallContext* kU_InQueryCall();

  class  OC_MatchContext : public antlr4::ParserRuleContext {
  public:
    OC_MatchContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MATCH();
    OC_PatternContext *oC_Pattern();
    antlr4::tree::TerminalNode *OPTIONAL();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_WhereContext *oC_Where();

   
  };

  OC_MatchContext* oC_Match();

  class  OC_UnwindContext : public antlr4::ParserRuleContext {
  public:
    OC_UnwindContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UNWIND();
    OC_ExpressionContext *oC_Expression();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *AS();
    OC_VariableContext *oC_Variable();

   
  };

  OC_UnwindContext* oC_Unwind();

  class  OC_CreateContext : public antlr4::ParserRuleContext {
  public:
    OC_CreateContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    OC_PatternContext *oC_Pattern();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_CreateContext* oC_Create();

  class  OC_MergeContext : public antlr4::ParserRuleContext {
  public:
    OC_MergeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MERGE();
    OC_PatternContext *oC_Pattern();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_MergeActionContext *> oC_MergeAction();
    OC_MergeActionContext* oC_MergeAction(size_t i);

   
  };

  OC_MergeContext* oC_Merge();

  class  OC_MergeActionContext : public antlr4::ParserRuleContext {
  public:
    OC_MergeActionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ON();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *MATCH();
    OC_SetContext *oC_Set();
    antlr4::tree::TerminalNode *CREATE();

   
  };

  OC_MergeActionContext* oC_MergeAction();

  class  OC_SetContext : public antlr4::ParserRuleContext {
  public:
    OC_SetContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SET();
    std::vector<OC_SetItemContext *> oC_SetItem();
    OC_SetItemContext* oC_SetItem(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_SetContext* oC_Set();

  class  OC_SetItemContext : public antlr4::ParserRuleContext {
  public:
    OC_SetItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_PropertyExpressionContext *oC_PropertyExpression();
    OC_ExpressionContext *oC_Expression();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_SetItemContext* oC_SetItem();

  class  OC_DeleteContext : public antlr4::ParserRuleContext {
  public:
    OC_DeleteContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DELETE();
    std::vector<OC_ExpressionContext *> oC_Expression();
    OC_ExpressionContext* oC_Expression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_DeleteContext* oC_Delete();

  class  OC_WithContext : public antlr4::ParserRuleContext {
  public:
    OC_WithContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WITH();
    OC_ProjectionBodyContext *oC_ProjectionBody();
    OC_WhereContext *oC_Where();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_WithContext* oC_With();

  class  OC_ReturnContext : public antlr4::ParserRuleContext {
  public:
    OC_ReturnContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RETURN();
    OC_ProjectionBodyContext *oC_ProjectionBody();

   
  };

  OC_ReturnContext* oC_Return();

  class  OC_ProjectionBodyContext : public antlr4::ParserRuleContext {
  public:
    OC_ProjectionBodyContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_ProjectionItemsContext *oC_ProjectionItems();
    antlr4::tree::TerminalNode *DISTINCT();
    OC_OrderContext *oC_Order();
    OC_SkipContext *oC_Skip();
    OC_LimitContext *oC_Limit();

   
  };

  OC_ProjectionBodyContext* oC_ProjectionBody();

  class  OC_ProjectionItemsContext : public antlr4::ParserRuleContext {
  public:
    OC_ProjectionItemsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *STAR();
    std::vector<OC_ProjectionItemContext *> oC_ProjectionItem();
    OC_ProjectionItemContext* oC_ProjectionItem(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_ProjectionItemsContext* oC_ProjectionItems();

  class  OC_ProjectionItemContext : public antlr4::ParserRuleContext {
  public:
    OC_ProjectionItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ExpressionContext *oC_Expression();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *AS();
    OC_VariableContext *oC_Variable();

   
  };

  OC_ProjectionItemContext* oC_ProjectionItem();

  class  OC_OrderContext : public antlr4::ParserRuleContext {
  public:
    OC_OrderContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *ORDER();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *BY();
    std::vector<OC_SortItemContext *> oC_SortItem();
    OC_SortItemContext* oC_SortItem(size_t i);

   
  };

  OC_OrderContext* oC_Order();

  class  OC_SkipContext : public antlr4::ParserRuleContext {
  public:
    OC_SkipContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *L_SKIP();
    antlr4::tree::TerminalNode *SP();
    OC_ExpressionContext *oC_Expression();

   
  };

  OC_SkipContext* oC_Skip();

  class  OC_LimitContext : public antlr4::ParserRuleContext {
  public:
    OC_LimitContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LIMIT();
    antlr4::tree::TerminalNode *SP();
    OC_ExpressionContext *oC_Expression();

   
  };

  OC_LimitContext* oC_Limit();

  class  OC_SortItemContext : public antlr4::ParserRuleContext {
  public:
    OC_SortItemContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ExpressionContext *oC_Expression();
    antlr4::tree::TerminalNode *ASCENDING();
    antlr4::tree::TerminalNode *ASC();
    antlr4::tree::TerminalNode *DESCENDING();
    antlr4::tree::TerminalNode *DESC();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_SortItemContext* oC_SortItem();

  class  OC_WhereContext : public antlr4::ParserRuleContext {
  public:
    OC_WhereContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WHERE();
    antlr4::tree::TerminalNode *SP();
    OC_ExpressionContext *oC_Expression();

   
  };

  OC_WhereContext* oC_Where();

  class  OC_PatternContext : public antlr4::ParserRuleContext {
  public:
    OC_PatternContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_PatternPartContext *> oC_PatternPart();
    OC_PatternPartContext* oC_PatternPart(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_PatternContext* oC_Pattern();

  class  OC_PatternPartContext : public antlr4::ParserRuleContext {
  public:
    OC_PatternPartContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_VariableContext *oC_Variable();
    OC_AnonymousPatternPartContext *oC_AnonymousPatternPart();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_PatternPartContext* oC_PatternPart();

  class  OC_AnonymousPatternPartContext : public antlr4::ParserRuleContext {
  public:
    OC_AnonymousPatternPartContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_PatternElementContext *oC_PatternElement();

   
  };

  OC_AnonymousPatternPartContext* oC_AnonymousPatternPart();

  class  OC_PatternElementContext : public antlr4::ParserRuleContext {
  public:
    OC_PatternElementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_NodePatternContext *oC_NodePattern();
    std::vector<OC_PatternElementChainContext *> oC_PatternElementChain();
    OC_PatternElementChainContext* oC_PatternElementChain(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_PatternElementContext *oC_PatternElement();

   
  };

  OC_PatternElementContext* oC_PatternElement();

  class  OC_NodePatternContext : public antlr4::ParserRuleContext {
  public:
    OC_NodePatternContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_VariableContext *oC_Variable();
    OC_NodeLabelsContext *oC_NodeLabels();
    KU_PropertiesContext *kU_Properties();

   
  };

  OC_NodePatternContext* oC_NodePattern();

  class  OC_PatternElementChainContext : public antlr4::ParserRuleContext {
  public:
    OC_PatternElementChainContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_RelationshipPatternContext *oC_RelationshipPattern();
    OC_NodePatternContext *oC_NodePattern();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_PatternElementChainContext* oC_PatternElementChain();

  class  OC_RelationshipPatternContext : public antlr4::ParserRuleContext {
  public:
    OC_RelationshipPatternContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_LeftArrowHeadContext *oC_LeftArrowHead();
    std::vector<OC_DashContext *> oC_Dash();
    OC_DashContext* oC_Dash(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_RelationshipDetailContext *oC_RelationshipDetail();
    OC_RightArrowHeadContext *oC_RightArrowHead();

   
  };

  OC_RelationshipPatternContext* oC_RelationshipPattern();

  class  OC_RelationshipDetailContext : public antlr4::ParserRuleContext {
  public:
    OC_RelationshipDetailContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_VariableContext *oC_Variable();
    OC_RelationshipTypesContext *oC_RelationshipTypes();
    OC_RangeLiteralContext *oC_RangeLiteral();
    KU_PropertiesContext *kU_Properties();

   
  };

  OC_RelationshipDetailContext* oC_RelationshipDetail();

  class  KU_PropertiesContext : public antlr4::ParserRuleContext {
  public:
    KU_PropertiesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_PropertyKeyNameContext *> oC_PropertyKeyName();
    OC_PropertyKeyNameContext* oC_PropertyKeyName(size_t i);
    std::vector<OC_ExpressionContext *> oC_Expression();
    OC_ExpressionContext* oC_Expression(size_t i);

   
  };

  KU_PropertiesContext* kU_Properties();

  class  OC_RelationshipTypesContext : public antlr4::ParserRuleContext {
  public:
    OC_RelationshipTypesContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_RelTypeNameContext *> oC_RelTypeName();
    OC_RelTypeNameContext* oC_RelTypeName(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_RelationshipTypesContext* oC_RelationshipTypes();

  class  OC_NodeLabelsContext : public antlr4::ParserRuleContext {
  public:
    OC_NodeLabelsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_NodeLabelContext *> oC_NodeLabel();
    OC_NodeLabelContext* oC_NodeLabel(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_NodeLabelsContext* oC_NodeLabels();

  class  OC_NodeLabelContext : public antlr4::ParserRuleContext {
  public:
    OC_NodeLabelContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_LabelNameContext *oC_LabelName();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_NodeLabelContext* oC_NodeLabel();

  class  OC_RangeLiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_RangeLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *STAR();
    std::vector<OC_IntegerLiteralContext *> oC_IntegerLiteral();
    OC_IntegerLiteralContext* oC_IntegerLiteral(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *SHORTEST();
    antlr4::tree::TerminalNode *ALL();
    OC_VariableContext *oC_Variable();
    OC_WhereContext *oC_Where();

   
  };

  OC_RangeLiteralContext* oC_RangeLiteral();

  class  OC_LabelNameContext : public antlr4::ParserRuleContext {
  public:
    OC_LabelNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SchemaNameContext *oC_SchemaName();

   
  };

  OC_LabelNameContext* oC_LabelName();

  class  OC_RelTypeNameContext : public antlr4::ParserRuleContext {
  public:
    OC_RelTypeNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SchemaNameContext *oC_SchemaName();

   
  };

  OC_RelTypeNameContext* oC_RelTypeName();

  class  OC_ExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_ExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_OrExpressionContext *oC_OrExpression();

   
  };

  OC_ExpressionContext* oC_Expression();

  class  OC_OrExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_OrExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_XorExpressionContext *> oC_XorExpression();
    OC_XorExpressionContext* oC_XorExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<antlr4::tree::TerminalNode *> OR();
    antlr4::tree::TerminalNode* OR(size_t i);

   
  };

  OC_OrExpressionContext* oC_OrExpression();

  class  OC_XorExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_XorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_AndExpressionContext *> oC_AndExpression();
    OC_AndExpressionContext* oC_AndExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<antlr4::tree::TerminalNode *> XOR();
    antlr4::tree::TerminalNode* XOR(size_t i);

   
  };

  OC_XorExpressionContext* oC_XorExpression();

  class  OC_AndExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_AndExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_NotExpressionContext *> oC_NotExpression();
    OC_NotExpressionContext* oC_NotExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<antlr4::tree::TerminalNode *> AND();
    antlr4::tree::TerminalNode* AND(size_t i);

   
  };

  OC_AndExpressionContext* oC_AndExpression();

  class  OC_NotExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_NotExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ComparisonExpressionContext *oC_ComparisonExpression();
    antlr4::tree::TerminalNode *NOT();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_NotExpressionContext* oC_NotExpression();

  class  OC_ComparisonExpressionContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *invalid_not_equalToken = nullptr;
    OC_ComparisonExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<KU_BitwiseOrOperatorExpressionContext *> kU_BitwiseOrOperatorExpression();
    KU_BitwiseOrOperatorExpressionContext* kU_BitwiseOrOperatorExpression(size_t i);
    std::vector<KU_ComparisonOperatorContext *> kU_ComparisonOperator();
    KU_ComparisonOperatorContext* kU_ComparisonOperator(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *INVALID_NOT_EQUAL();

   
  };

  OC_ComparisonExpressionContext* oC_ComparisonExpression();

  class  KU_ComparisonOperatorContext : public antlr4::ParserRuleContext {
  public:
    KU_ComparisonOperatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

   
  };

  KU_ComparisonOperatorContext* kU_ComparisonOperator();

  class  KU_BitwiseOrOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    KU_BitwiseOrOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<KU_BitwiseAndOperatorExpressionContext *> kU_BitwiseAndOperatorExpression();
    KU_BitwiseAndOperatorExpressionContext* kU_BitwiseAndOperatorExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  KU_BitwiseOrOperatorExpressionContext* kU_BitwiseOrOperatorExpression();

  class  KU_BitwiseAndOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    KU_BitwiseAndOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<KU_BitShiftOperatorExpressionContext *> kU_BitShiftOperatorExpression();
    KU_BitShiftOperatorExpressionContext* kU_BitShiftOperatorExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  KU_BitwiseAndOperatorExpressionContext* kU_BitwiseAndOperatorExpression();

  class  KU_BitShiftOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    KU_BitShiftOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_AddOrSubtractExpressionContext *> oC_AddOrSubtractExpression();
    OC_AddOrSubtractExpressionContext* oC_AddOrSubtractExpression(size_t i);
    std::vector<KU_BitShiftOperatorContext *> kU_BitShiftOperator();
    KU_BitShiftOperatorContext* kU_BitShiftOperator(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  KU_BitShiftOperatorExpressionContext* kU_BitShiftOperatorExpression();

  class  KU_BitShiftOperatorContext : public antlr4::ParserRuleContext {
  public:
    KU_BitShiftOperatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

   
  };

  KU_BitShiftOperatorContext* kU_BitShiftOperator();

  class  OC_AddOrSubtractExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_AddOrSubtractExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_MultiplyDivideModuloExpressionContext *> oC_MultiplyDivideModuloExpression();
    OC_MultiplyDivideModuloExpressionContext* oC_MultiplyDivideModuloExpression(size_t i);
    std::vector<KU_AddOrSubtractOperatorContext *> kU_AddOrSubtractOperator();
    KU_AddOrSubtractOperatorContext* kU_AddOrSubtractOperator(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_AddOrSubtractExpressionContext* oC_AddOrSubtractExpression();

  class  KU_AddOrSubtractOperatorContext : public antlr4::ParserRuleContext {
  public:
    KU_AddOrSubtractOperatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MINUS();

   
  };

  KU_AddOrSubtractOperatorContext* kU_AddOrSubtractOperator();

  class  OC_MultiplyDivideModuloExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_MultiplyDivideModuloExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_PowerOfExpressionContext *> oC_PowerOfExpression();
    OC_PowerOfExpressionContext* oC_PowerOfExpression(size_t i);
    std::vector<KU_MultiplyDivideModuloOperatorContext *> kU_MultiplyDivideModuloOperator();
    KU_MultiplyDivideModuloOperatorContext* kU_MultiplyDivideModuloOperator(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_MultiplyDivideModuloExpressionContext* oC_MultiplyDivideModuloExpression();

  class  KU_MultiplyDivideModuloOperatorContext : public antlr4::ParserRuleContext {
  public:
    KU_MultiplyDivideModuloOperatorContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *STAR();

   
  };

  KU_MultiplyDivideModuloOperatorContext* kU_MultiplyDivideModuloOperator();

  class  OC_PowerOfExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_PowerOfExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_UnaryAddSubtractOrFactorialExpressionContext *> oC_UnaryAddSubtractOrFactorialExpression();
    OC_UnaryAddSubtractOrFactorialExpressionContext* oC_UnaryAddSubtractOrFactorialExpression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_PowerOfExpressionContext* oC_PowerOfExpression();

  class  OC_UnaryAddSubtractOrFactorialExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_UnaryAddSubtractOrFactorialExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_StringListNullOperatorExpressionContext *oC_StringListNullOperatorExpression();
    antlr4::tree::TerminalNode *MINUS();
    antlr4::tree::TerminalNode *FACTORIAL();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_UnaryAddSubtractOrFactorialExpressionContext* oC_UnaryAddSubtractOrFactorialExpression();

  class  OC_StringListNullOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_StringListNullOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_PropertyOrLabelsExpressionContext *oC_PropertyOrLabelsExpression();
    OC_StringOperatorExpressionContext *oC_StringOperatorExpression();
    OC_NullOperatorExpressionContext *oC_NullOperatorExpression();
    std::vector<OC_ListOperatorExpressionContext *> oC_ListOperatorExpression();
    OC_ListOperatorExpressionContext* oC_ListOperatorExpression(size_t i);

   
  };

  OC_StringListNullOperatorExpressionContext* oC_StringListNullOperatorExpression();

  class  OC_ListOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_ListOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    KU_ListExtractOperatorExpressionContext *kU_ListExtractOperatorExpression();
    KU_ListSliceOperatorExpressionContext *kU_ListSliceOperatorExpression();

   
  };

  OC_ListOperatorExpressionContext* oC_ListOperatorExpression();

  class  KU_ListExtractOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    KU_ListExtractOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ExpressionContext *oC_Expression();

   
  };

  KU_ListExtractOperatorExpressionContext* kU_ListExtractOperatorExpression();

  class  KU_ListSliceOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    KU_ListSliceOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_ExpressionContext *> oC_Expression();
    OC_ExpressionContext* oC_Expression(size_t i);

   
  };

  KU_ListSliceOperatorExpressionContext* kU_ListSliceOperatorExpression();

  class  OC_StringOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_StringOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_PropertyOrLabelsExpressionContext *oC_PropertyOrLabelsExpression();
    OC_RegularExpressionContext *oC_RegularExpression();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *STARTS();
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *ENDS();
    antlr4::tree::TerminalNode *CONTAINS();

   
  };

  OC_StringOperatorExpressionContext* oC_StringOperatorExpression();

  class  OC_RegularExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_RegularExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_RegularExpressionContext* oC_RegularExpression();

  class  OC_NullOperatorExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_NullOperatorExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *IS();
    antlr4::tree::TerminalNode *NULL_();
    antlr4::tree::TerminalNode *NOT();

   
  };

  OC_NullOperatorExpressionContext* oC_NullOperatorExpression();

  class  OC_PropertyOrLabelsExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_PropertyOrLabelsExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_AtomContext *oC_Atom();
    std::vector<OC_PropertyLookupContext *> oC_PropertyLookup();
    OC_PropertyLookupContext* oC_PropertyLookup(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_PropertyOrLabelsExpressionContext* oC_PropertyOrLabelsExpression();

  class  OC_AtomContext : public antlr4::ParserRuleContext {
  public:
    OC_AtomContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_LiteralContext *oC_Literal();
    OC_ParameterContext *oC_Parameter();
    OC_CaseExpressionContext *oC_CaseExpression();
    OC_ParenthesizedExpressionContext *oC_ParenthesizedExpression();
    OC_FunctionInvocationContext *oC_FunctionInvocation();
    OC_ExistentialSubqueryContext *oC_ExistentialSubquery();
    OC_VariableContext *oC_Variable();

   
  };

  OC_AtomContext* oC_Atom();

  class  OC_LiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_LiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_NumberLiteralContext *oC_NumberLiteral();
    antlr4::tree::TerminalNode *StringLiteral();
    OC_BooleanLiteralContext *oC_BooleanLiteral();
    antlr4::tree::TerminalNode *NULL_();
    OC_ListLiteralContext *oC_ListLiteral();
    KU_StructLiteralContext *kU_StructLiteral();

   
  };

  OC_LiteralContext* oC_Literal();

  class  OC_BooleanLiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_BooleanLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *TRUE();
    antlr4::tree::TerminalNode *FALSE();

   
  };

  OC_BooleanLiteralContext* oC_BooleanLiteral();

  class  OC_ListLiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_ListLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_ExpressionContext *> oC_Expression();
    OC_ExpressionContext* oC_Expression(size_t i);

   
  };

  OC_ListLiteralContext* oC_ListLiteral();

  class  KU_StructLiteralContext : public antlr4::ParserRuleContext {
  public:
    KU_StructLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<KU_StructFieldContext *> kU_StructField();
    KU_StructFieldContext* kU_StructField(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  KU_StructLiteralContext* kU_StructLiteral();

  class  KU_StructFieldContext : public antlr4::ParserRuleContext {
  public:
    KU_StructFieldContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ExpressionContext *oC_Expression();
    OC_SymbolicNameContext *oC_SymbolicName();
    antlr4::tree::TerminalNode *StringLiteral();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  KU_StructFieldContext* kU_StructField();

  class  OC_ParenthesizedExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_ParenthesizedExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ExpressionContext *oC_Expression();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_ParenthesizedExpressionContext* oC_ParenthesizedExpression();

  class  OC_FunctionInvocationContext : public antlr4::ParserRuleContext {
  public:
    OC_FunctionInvocationContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_FunctionNameContext *oC_FunctionName();
    antlr4::tree::TerminalNode *STAR();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *DISTINCT();
    std::vector<KU_FunctionParameterContext *> kU_FunctionParameter();
    KU_FunctionParameterContext* kU_FunctionParameter(size_t i);

   
  };

  OC_FunctionInvocationContext* oC_FunctionInvocation();

  class  OC_FunctionNameContext : public antlr4::ParserRuleContext {
  public:
    OC_FunctionNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();

   
  };

  OC_FunctionNameContext* oC_FunctionName();

  class  KU_FunctionParameterContext : public antlr4::ParserRuleContext {
  public:
    KU_FunctionParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_ExpressionContext *oC_Expression();
    OC_SymbolicNameContext *oC_SymbolicName();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  KU_FunctionParameterContext* kU_FunctionParameter();

  class  OC_ExistentialSubqueryContext : public antlr4::ParserRuleContext {
  public:
    OC_ExistentialSubqueryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *EXISTS();
    antlr4::tree::TerminalNode *MATCH();
    OC_PatternContext *oC_Pattern();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_WhereContext *oC_Where();

   
  };

  OC_ExistentialSubqueryContext* oC_ExistentialSubquery();

  class  OC_PropertyLookupContext : public antlr4::ParserRuleContext {
  public:
    OC_PropertyLookupContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_PropertyKeyNameContext *oC_PropertyKeyName();
    antlr4::tree::TerminalNode *STAR();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_PropertyLookupContext* oC_PropertyLookup();

  class  OC_CaseExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_CaseExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *END();
    antlr4::tree::TerminalNode *ELSE();
    std::vector<OC_ExpressionContext *> oC_Expression();
    OC_ExpressionContext* oC_Expression(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *CASE();
    std::vector<OC_CaseAlternativeContext *> oC_CaseAlternative();
    OC_CaseAlternativeContext* oC_CaseAlternative(size_t i);

   
  };

  OC_CaseExpressionContext* oC_CaseExpression();

  class  OC_CaseAlternativeContext : public antlr4::ParserRuleContext {
  public:
    OC_CaseAlternativeContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *WHEN();
    std::vector<OC_ExpressionContext *> oC_Expression();
    OC_ExpressionContext* oC_Expression(size_t i);
    antlr4::tree::TerminalNode *THEN();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);

   
  };

  OC_CaseAlternativeContext* oC_CaseAlternative();

  class  OC_VariableContext : public antlr4::ParserRuleContext {
  public:
    OC_VariableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();

   
  };

  OC_VariableContext* oC_Variable();

  class  OC_NumberLiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_NumberLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_DoubleLiteralContext *oC_DoubleLiteral();
    OC_IntegerLiteralContext *oC_IntegerLiteral();

   
  };

  OC_NumberLiteralContext* oC_NumberLiteral();

  class  OC_ParameterContext : public antlr4::ParserRuleContext {
  public:
    OC_ParameterContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();
    antlr4::tree::TerminalNode *DecimalInteger();

   
  };

  OC_ParameterContext* oC_Parameter();

  class  OC_PropertyExpressionContext : public antlr4::ParserRuleContext {
  public:
    OC_PropertyExpressionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_AtomContext *oC_Atom();
    OC_PropertyLookupContext *oC_PropertyLookup();
    antlr4::tree::TerminalNode *SP();

   
  };

  OC_PropertyExpressionContext* oC_PropertyExpression();

  class  OC_PropertyKeyNameContext : public antlr4::ParserRuleContext {
  public:
    OC_PropertyKeyNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SchemaNameContext *oC_SchemaName();

   
  };

  OC_PropertyKeyNameContext* oC_PropertyKeyName();

  class  OC_IntegerLiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_IntegerLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DecimalInteger();

   
  };

  OC_IntegerLiteralContext* oC_IntegerLiteral();

  class  OC_DoubleLiteralContext : public antlr4::ParserRuleContext {
  public:
    OC_DoubleLiteralContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *RegularDecimalReal();

   
  };

  OC_DoubleLiteralContext* oC_DoubleLiteral();

  class  OC_SchemaNameContext : public antlr4::ParserRuleContext {
  public:
    OC_SchemaNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_SymbolicNameContext *oC_SymbolicName();

   
  };

  OC_SchemaNameContext* oC_SchemaName();

  class  OC_SymbolicNameContext : public antlr4::ParserRuleContext {
  public:
    antlr4::Token *escapedsymbolicnameToken = nullptr;
    OC_SymbolicNameContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *UnescapedSymbolicName();
    antlr4::tree::TerminalNode *EscapedSymbolicName();
    antlr4::tree::TerminalNode *HexLetter();

   
  };

  OC_SymbolicNameContext* oC_SymbolicName();

  class  OC_LeftArrowHeadContext : public antlr4::ParserRuleContext {
  public:
    OC_LeftArrowHeadContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

   
  };

  OC_LeftArrowHeadContext* oC_LeftArrowHead();

  class  OC_RightArrowHeadContext : public antlr4::ParserRuleContext {
  public:
    OC_RightArrowHeadContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;

   
  };

  OC_RightArrowHeadContext* oC_RightArrowHead();

  class  OC_DashContext : public antlr4::ParserRuleContext {
  public:
    OC_DashContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *MINUS();

   
  };

  OC_DashContext* oC_Dash();


private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


      virtual void notifyQueryNotConcludeWithReturn(antlr4::Token* startToken) {};
      virtual void notifyNodePatternWithoutParentheses(std::string nodeName, antlr4::Token* startToken) {};
      virtual void notifyInvalidNotEqualOperator(antlr4::Token* startToken) {};
      virtual void notifyEmptyToken(antlr4::Token* startToken) {};
      virtual void notifyReturnNotAtEnd(antlr4::Token* startToken) {};
      virtual void notifyNonBinaryComparison(antlr4::Token* startToken) {};


  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

