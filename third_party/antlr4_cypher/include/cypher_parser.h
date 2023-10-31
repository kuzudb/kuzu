
// Generated from Cypher.g4 by ANTLR 4.13.1

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
    T__44 = 45, T__45 = 46, CALL = 47, COMMENT = 48, MACRO = 49, GLOB = 50, 
    COPY = 51, FROM = 52, COLUMN = 53, NODE = 54, TABLE = 55, GROUP = 56, 
    RDF = 57, GRAPH = 58, DROP = 59, ALTER = 60, DEFAULT = 61, RENAME = 62, 
    ADD = 63, PRIMARY = 64, KEY = 65, REL = 66, TO = 67, EXPLAIN = 68, PROFILE = 69, 
    BEGIN = 70, TRANSACTION = 71, READ = 72, ONLY = 73, WRITE = 74, COMMIT = 75, 
    COMMIT_SKIP_CHECKPOINT = 76, ROLLBACK = 77, ROLLBACK_SKIP_CHECKPOINT = 78, 
    UNION = 79, ALL = 80, LOAD = 81, HEADERS = 82, OPTIONAL = 83, MATCH = 84, 
    UNWIND = 85, CREATE = 86, MERGE = 87, ON = 88, SET = 89, DELETE = 90, 
    WITH = 91, RETURN = 92, DISTINCT = 93, STAR = 94, AS = 95, ORDER = 96, 
    BY = 97, L_SKIP = 98, LIMIT = 99, ASCENDING = 100, ASC = 101, DESCENDING = 102, 
    DESC = 103, WHERE = 104, SHORTEST = 105, OR = 106, XOR = 107, AND = 108, 
    NOT = 109, INVALID_NOT_EQUAL = 110, MINUS = 111, FACTORIAL = 112, STARTS = 113, 
    ENDS = 114, CONTAINS = 115, IS = 116, NULL_ = 117, TRUE = 118, FALSE = 119, 
    EXISTS = 120, CASE = 121, ELSE = 122, END = 123, WHEN = 124, THEN = 125, 
    StringLiteral = 126, EscapedChar = 127, DecimalInteger = 128, HexLetter = 129, 
    HexDigit = 130, Digit = 131, NonZeroDigit = 132, NonZeroOctDigit = 133, 
    ZeroDigit = 134, RegularDecimalReal = 135, UnescapedSymbolicName = 136, 
    IdentifierStart = 137, IdentifierPart = 138, EscapedSymbolicName = 139, 
    SP = 140, WHITESPACE = 141, Comment = 142, Unknown = 143
  };

  enum {
    RuleOC_Cypher = 0, RuleOC_Statement = 1, RuleKU_CopyFrom = 2, RuleKU_CopyFromByColumn = 3, 
    RuleKU_CopyTO = 4, RuleKU_StandaloneCall = 5, RuleKU_CommentOn = 6, 
    RuleKU_CreateMacro = 7, RuleKU_PositionalArgs = 8, RuleKU_DefaultArg = 9, 
    RuleKU_FilePaths = 10, RuleKU_ParsingOptions = 11, RuleKU_ParsingOption = 12, 
    RuleKU_DDL = 13, RuleKU_CreateNodeTable = 14, RuleKU_CreateRelTable = 15, 
    RuleKU_CreateRelTableGroup = 16, RuleKU_RelTableConnection = 17, RuleKU_CreateRdfGraph = 18, 
    RuleKU_DropTable = 19, RuleKU_AlterTable = 20, RuleKU_AlterOptions = 21, 
    RuleKU_AddProperty = 22, RuleKU_DropProperty = 23, RuleKU_RenameTable = 24, 
    RuleKU_RenameProperty = 25, RuleKU_PropertyDefinitions = 26, RuleKU_PropertyDefinition = 27, 
    RuleKU_CreateNodeConstraint = 28, RuleKU_DataType = 29, RuleKU_ListIdentifiers = 30, 
    RuleKU_ListIdentifier = 31, RuleOC_AnyCypherOption = 32, RuleOC_Explain = 33, 
    RuleOC_Profile = 34, RuleKU_Transaction = 35, RuleOC_Query = 36, RuleOC_RegularQuery = 37, 
    RuleOC_Union = 38, RuleOC_SingleQuery = 39, RuleOC_SinglePartQuery = 40, 
    RuleOC_MultiPartQuery = 41, RuleKU_QueryPart = 42, RuleOC_UpdatingClause = 43, 
    RuleOC_ReadingClause = 44, RuleKU_LoadFrom = 45, RuleKU_InQueryCall = 46, 
    RuleOC_Match = 47, RuleOC_Unwind = 48, RuleOC_Create = 49, RuleOC_Merge = 50, 
    RuleOC_MergeAction = 51, RuleOC_Set = 52, RuleOC_SetItem = 53, RuleOC_Delete = 54, 
    RuleOC_With = 55, RuleOC_Return = 56, RuleOC_ProjectionBody = 57, RuleOC_ProjectionItems = 58, 
    RuleOC_ProjectionItem = 59, RuleOC_Order = 60, RuleOC_Skip = 61, RuleOC_Limit = 62, 
    RuleOC_SortItem = 63, RuleOC_Where = 64, RuleOC_Pattern = 65, RuleOC_PatternPart = 66, 
    RuleOC_AnonymousPatternPart = 67, RuleOC_PatternElement = 68, RuleOC_NodePattern = 69, 
    RuleOC_PatternElementChain = 70, RuleOC_RelationshipPattern = 71, RuleOC_RelationshipDetail = 72, 
    RuleKU_Properties = 73, RuleOC_RelationshipTypes = 74, RuleOC_NodeLabels = 75, 
    RuleOC_NodeLabel = 76, RuleOC_RangeLiteral = 77, RuleKU_RecursiveRelationshipComprehension = 78, 
    RuleKU_IntermediateNodeProjectionItems = 79, RuleKU_IntermediateRelProjectionItems = 80, 
    RuleOC_LowerBound = 81, RuleOC_UpperBound = 82, RuleOC_LabelName = 83, 
    RuleOC_RelTypeName = 84, RuleOC_Expression = 85, RuleOC_OrExpression = 86, 
    RuleOC_XorExpression = 87, RuleOC_AndExpression = 88, RuleOC_NotExpression = 89, 
    RuleOC_ComparisonExpression = 90, RuleKU_ComparisonOperator = 91, RuleKU_BitwiseOrOperatorExpression = 92, 
    RuleKU_BitwiseAndOperatorExpression = 93, RuleKU_BitShiftOperatorExpression = 94, 
    RuleKU_BitShiftOperator = 95, RuleOC_AddOrSubtractExpression = 96, RuleKU_AddOrSubtractOperator = 97, 
    RuleOC_MultiplyDivideModuloExpression = 98, RuleKU_MultiplyDivideModuloOperator = 99, 
    RuleOC_PowerOfExpression = 100, RuleOC_UnaryAddSubtractOrFactorialExpression = 101, 
    RuleOC_StringListNullOperatorExpression = 102, RuleOC_ListOperatorExpression = 103, 
    RuleKU_ListExtractOperatorExpression = 104, RuleKU_ListSliceOperatorExpression = 105, 
    RuleOC_StringOperatorExpression = 106, RuleOC_RegularExpression = 107, 
    RuleOC_NullOperatorExpression = 108, RuleOC_PropertyOrLabelsExpression = 109, 
    RuleOC_Atom = 110, RuleOC_Literal = 111, RuleOC_BooleanLiteral = 112, 
    RuleOC_ListLiteral = 113, RuleKU_ListEntry = 114, RuleKU_StructLiteral = 115, 
    RuleKU_StructField = 116, RuleOC_ParenthesizedExpression = 117, RuleOC_FunctionInvocation = 118, 
    RuleOC_FunctionName = 119, RuleKU_FunctionParameter = 120, RuleOC_ExistentialSubquery = 121, 
    RuleOC_PropertyLookup = 122, RuleOC_CaseExpression = 123, RuleOC_CaseAlternative = 124, 
    RuleOC_Variable = 125, RuleOC_NumberLiteral = 126, RuleOC_Parameter = 127, 
    RuleOC_PropertyExpression = 128, RuleOC_PropertyKeyName = 129, RuleOC_IntegerLiteral = 130, 
    RuleOC_DoubleLiteral = 131, RuleOC_SchemaName = 132, RuleOC_SymbolicName = 133, 
    RuleKU_NonReservedKeywords = 134, RuleOC_LeftArrowHead = 135, RuleOC_RightArrowHead = 136, 
    RuleOC_Dash = 137
  };

  explicit CypherParser(antlr4::TokenStream *input);

  CypherParser(antlr4::TokenStream *input, const antlr4::atn::ParserATNSimulatorOptions &options);

  ~CypherParser() override;

  std::string getGrammarFileName() const override;

  const antlr4::atn::ATN& getATN() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;


  class OC_CypherContext;
  class OC_StatementContext;
  class KU_CopyFromContext;
  class KU_CopyFromByColumnContext;
  class KU_CopyTOContext;
  class KU_StandaloneCallContext;
  class KU_CommentOnContext;
  class KU_CreateMacroContext;
  class KU_PositionalArgsContext;
  class KU_DefaultArgContext;
  class KU_FilePathsContext;
  class KU_ParsingOptionsContext;
  class KU_ParsingOptionContext;
  class KU_DDLContext;
  class KU_CreateNodeTableContext;
  class KU_CreateRelTableContext;
  class KU_CreateRelTableGroupContext;
  class KU_RelTableConnectionContext;
  class KU_CreateRdfGraphContext;
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
  class KU_TransactionContext;
  class OC_QueryContext;
  class OC_RegularQueryContext;
  class OC_UnionContext;
  class OC_SingleQueryContext;
  class OC_SinglePartQueryContext;
  class OC_MultiPartQueryContext;
  class KU_QueryPartContext;
  class OC_UpdatingClauseContext;
  class OC_ReadingClauseContext;
  class KU_LoadFromContext;
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
  class KU_RecursiveRelationshipComprehensionContext;
  class KU_IntermediateNodeProjectionItemsContext;
  class KU_IntermediateRelProjectionItemsContext;
  class OC_LowerBoundContext;
  class OC_UpperBoundContext;
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
  class KU_ListEntryContext;
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
  class KU_NonReservedKeywordsContext;
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

  class  OC_StatementContext : public antlr4::ParserRuleContext {
  public:
    OC_StatementContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    OC_QueryContext *oC_Query();
    KU_DDLContext *kU_DDL();
    KU_CopyFromContext *kU_CopyFrom();
    KU_CopyFromByColumnContext *kU_CopyFromByColumn();
    KU_CopyTOContext *kU_CopyTO();
    KU_StandaloneCallContext *kU_StandaloneCall();
    KU_CreateMacroContext *kU_CreateMacro();
    KU_CommentOnContext *kU_CommentOn();
    KU_TransactionContext *kU_Transaction();

   
  };

  OC_StatementContext* oC_Statement();

  class  KU_CopyFromContext : public antlr4::ParserRuleContext {
  public:
    KU_CopyFromContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COPY();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_SchemaNameContext *oC_SchemaName();
    antlr4::tree::TerminalNode *FROM();
    KU_FilePathsContext *kU_FilePaths();
    KU_ParsingOptionsContext *kU_ParsingOptions();

   
  };

  KU_CopyFromContext* kU_CopyFrom();

  class  KU_CopyFromByColumnContext : public antlr4::ParserRuleContext {
  public:
    KU_CopyFromByColumnContext(antlr4::ParserRuleContext *parent, size_t invokingState);
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

  KU_CopyFromByColumnContext* kU_CopyFromByColumn();

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

  class  KU_CommentOnContext : public antlr4::ParserRuleContext {
  public:
    KU_CommentOnContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COMMENT();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *ON();
    antlr4::tree::TerminalNode *TABLE();
    OC_SchemaNameContext *oC_SchemaName();
    antlr4::tree::TerminalNode *IS();
    antlr4::tree::TerminalNode *StringLiteral();

   
  };

  KU_CommentOnContext* kU_CommentOn();

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
    KU_CreateNodeTableContext *kU_CreateNodeTable();
    KU_CreateRelTableContext *kU_CreateRelTable();
    KU_CreateRelTableGroupContext *kU_CreateRelTableGroup();
    KU_CreateRdfGraphContext *kU_CreateRdfGraph();
    KU_DropTableContext *kU_DropTable();
    KU_AlterTableContext *kU_AlterTable();

   
  };

  KU_DDLContext* kU_DDL();

  class  KU_CreateNodeTableContext : public antlr4::ParserRuleContext {
  public:
    KU_CreateNodeTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
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

  KU_CreateNodeTableContext* kU_CreateNodeTable();

  class  KU_CreateRelTableContext : public antlr4::ParserRuleContext {
  public:
    KU_CreateRelTableContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *REL();
    antlr4::tree::TerminalNode *TABLE();
    OC_SchemaNameContext *oC_SchemaName();
    KU_RelTableConnectionContext *kU_RelTableConnection();
    KU_PropertyDefinitionsContext *kU_PropertyDefinitions();
    OC_SymbolicNameContext *oC_SymbolicName();

   
  };

  KU_CreateRelTableContext* kU_CreateRelTable();

  class  KU_CreateRelTableGroupContext : public antlr4::ParserRuleContext {
  public:
    KU_CreateRelTableGroupContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *REL();
    antlr4::tree::TerminalNode *TABLE();
    antlr4::tree::TerminalNode *GROUP();
    OC_SchemaNameContext *oC_SchemaName();
    std::vector<KU_RelTableConnectionContext *> kU_RelTableConnection();
    KU_RelTableConnectionContext* kU_RelTableConnection(size_t i);
    KU_PropertyDefinitionsContext *kU_PropertyDefinitions();
    OC_SymbolicNameContext *oC_SymbolicName();

   
  };

  KU_CreateRelTableGroupContext* kU_CreateRelTableGroup();

  class  KU_RelTableConnectionContext : public antlr4::ParserRuleContext {
  public:
    KU_RelTableConnectionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *FROM();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<OC_SchemaNameContext *> oC_SchemaName();
    OC_SchemaNameContext* oC_SchemaName(size_t i);
    antlr4::tree::TerminalNode *TO();

   
  };

  KU_RelTableConnectionContext* kU_RelTableConnection();

  class  KU_CreateRdfGraphContext : public antlr4::ParserRuleContext {
  public:
    KU_CreateRdfGraphContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CREATE();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *RDF();
    antlr4::tree::TerminalNode *GRAPH();
    OC_SchemaNameContext *oC_SchemaName();

   
  };

  KU_CreateRdfGraphContext* kU_CreateRdfGraph();

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
    antlr4::tree::TerminalNode *UNION();
    KU_PropertyDefinitionsContext *kU_PropertyDefinitions();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    std::vector<KU_DataTypeContext *> kU_DataType();
    KU_DataTypeContext* kU_DataType(size_t i);
    KU_ListIdentifiersContext *kU_ListIdentifiers();

   
  };

  KU_DataTypeContext* kU_DataType();
  KU_DataTypeContext* kU_DataType(int precedence);
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

  class  KU_TransactionContext : public antlr4::ParserRuleContext {
  public:
    KU_TransactionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *BEGIN();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *TRANSACTION();
    antlr4::tree::TerminalNode *READ();
    antlr4::tree::TerminalNode *ONLY();
    antlr4::tree::TerminalNode *COMMIT();
    antlr4::tree::TerminalNode *COMMIT_SKIP_CHECKPOINT();
    antlr4::tree::TerminalNode *ROLLBACK();
    antlr4::tree::TerminalNode *ROLLBACK_SKIP_CHECKPOINT();

   
  };

  KU_TransactionContext* kU_Transaction();

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
    KU_LoadFromContext *kU_LoadFrom();

   
  };

  OC_ReadingClauseContext* oC_ReadingClause();

  class  KU_LoadFromContext : public antlr4::ParserRuleContext {
  public:
    KU_LoadFromContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *LOAD();
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *FROM();
    KU_FilePathsContext *kU_FilePaths();
    antlr4::tree::TerminalNode *WITH();
    antlr4::tree::TerminalNode *HEADERS();
    KU_PropertyDefinitionsContext *kU_PropertyDefinitions();
    KU_ParsingOptionsContext *kU_ParsingOptions();
    OC_WhereContext *oC_Where();

   
  };

  KU_LoadFromContext* kU_LoadFrom();

  class  KU_InQueryCallContext : public antlr4::ParserRuleContext {
  public:
    KU_InQueryCallContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *CALL();
    antlr4::tree::TerminalNode *SP();
    OC_FunctionInvocationContext *oC_FunctionInvocation();

   
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
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    antlr4::tree::TerminalNode *SHORTEST();
    antlr4::tree::TerminalNode *ALL();
    OC_IntegerLiteralContext *oC_IntegerLiteral();
    KU_RecursiveRelationshipComprehensionContext *kU_RecursiveRelationshipComprehension();
    OC_LowerBoundContext *oC_LowerBound();
    OC_UpperBoundContext *oC_UpperBound();

   
  };

  OC_RangeLiteralContext* oC_RangeLiteral();

  class  KU_RecursiveRelationshipComprehensionContext : public antlr4::ParserRuleContext {
  public:
    KU_RecursiveRelationshipComprehensionContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<OC_VariableContext *> oC_Variable();
    OC_VariableContext* oC_Variable(size_t i);
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_WhereContext *oC_Where();
    KU_IntermediateRelProjectionItemsContext *kU_IntermediateRelProjectionItems();
    KU_IntermediateNodeProjectionItemsContext *kU_IntermediateNodeProjectionItems();

   
  };

  KU_RecursiveRelationshipComprehensionContext* kU_RecursiveRelationshipComprehension();

  class  KU_IntermediateNodeProjectionItemsContext : public antlr4::ParserRuleContext {
  public:
    KU_IntermediateNodeProjectionItemsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_ProjectionItemsContext *oC_ProjectionItems();

   
  };

  KU_IntermediateNodeProjectionItemsContext* kU_IntermediateNodeProjectionItems();

  class  KU_IntermediateRelProjectionItemsContext : public antlr4::ParserRuleContext {
  public:
    KU_IntermediateRelProjectionItemsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    std::vector<antlr4::tree::TerminalNode *> SP();
    antlr4::tree::TerminalNode* SP(size_t i);
    OC_ProjectionItemsContext *oC_ProjectionItems();

   
  };

  KU_IntermediateRelProjectionItemsContext* kU_IntermediateRelProjectionItems();

  class  OC_LowerBoundContext : public antlr4::ParserRuleContext {
  public:
    OC_LowerBoundContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DecimalInteger();

   
  };

  OC_LowerBoundContext* oC_LowerBound();

  class  OC_UpperBoundContext : public antlr4::ParserRuleContext {
  public:
    OC_UpperBoundContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *DecimalInteger();

   
  };

  OC_UpperBoundContext* oC_UpperBound();

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
    OC_ExpressionContext *oC_Expression();
    std::vector<KU_ListEntryContext *> kU_ListEntry();
    KU_ListEntryContext* kU_ListEntry(size_t i);

   
  };

  OC_ListLiteralContext* oC_ListLiteral();

  class  KU_ListEntryContext : public antlr4::ParserRuleContext {
  public:
    KU_ListEntryContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *SP();
    OC_ExpressionContext *oC_Expression();

   
  };

  KU_ListEntryContext* kU_ListEntry();

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
    KU_NonReservedKeywordsContext *kU_NonReservedKeywords();

   
  };

  OC_SymbolicNameContext* oC_SymbolicName();

  class  KU_NonReservedKeywordsContext : public antlr4::ParserRuleContext {
  public:
    KU_NonReservedKeywordsContext(antlr4::ParserRuleContext *parent, size_t invokingState);
    virtual size_t getRuleIndex() const override;
    antlr4::tree::TerminalNode *COMMENT();

   
  };

  KU_NonReservedKeywordsContext* kU_NonReservedKeywords();

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


  bool sempred(antlr4::RuleContext *_localctx, size_t ruleIndex, size_t predicateIndex) override;

  bool kU_DataTypeSempred(KU_DataTypeContext *_localctx, size_t predicateIndex);

  // By default the static state used to implement the parser is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

      virtual void notifyQueryNotConcludeWithReturn(antlr4::Token* startToken) {};
      virtual void notifyNodePatternWithoutParentheses(std::string nodeName, antlr4::Token* startToken) {};
      virtual void notifyInvalidNotEqualOperator(antlr4::Token* startToken) {};
      virtual void notifyEmptyToken(antlr4::Token* startToken) {};
      virtual void notifyReturnNotAtEnd(antlr4::Token* startToken) {};
      virtual void notifyNonBinaryComparison(antlr4::Token* startToken) {};

};

