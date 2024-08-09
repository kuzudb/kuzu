
// Generated from Cypher.g4 by ANTLR 4.13.1

#pragma once


#include "antlr4-runtime.h"




class  CypherLexer : public antlr4::Lexer {
public:
  enum {
    T__0 = 1, T__1 = 2, T__2 = 3, T__3 = 4, T__4 = 5, T__5 = 6, T__6 = 7, 
    T__7 = 8, T__8 = 9, T__9 = 10, T__10 = 11, T__11 = 12, T__12 = 13, T__13 = 14, 
    T__14 = 15, T__15 = 16, T__16 = 17, T__17 = 18, T__18 = 19, T__19 = 20, 
    T__20 = 21, T__21 = 22, T__22 = 23, T__23 = 24, T__24 = 25, T__25 = 26, 
    T__26 = 27, T__27 = 28, T__28 = 29, T__29 = 30, T__30 = 31, T__31 = 32, 
    T__32 = 33, T__33 = 34, T__34 = 35, T__35 = 36, T__36 = 37, T__37 = 38, 
    T__38 = 39, T__39 = 40, T__40 = 41, T__41 = 42, T__42 = 43, T__43 = 44, 
    T__44 = 45, ADD = 46, ALL = 47, ALTER = 48, AND = 49, AS = 50, ASC = 51, 
    ASCENDING = 52, ATTACH = 53, BEGIN = 54, BY = 55, CALL = 56, CASE = 57, 
    CAST = 58, CHECKPOINT = 59, COLUMN = 60, COMMENT = 61, COMMIT = 62, 
    COMMIT_SKIP_CHECKPOINT = 63, CONTAINS = 64, COPY = 65, COUNT = 66, CREATE = 67, 
    CYCLE = 68, DATABASE = 69, DBTYPE = 70, DEFAULT = 71, DELETE = 72, DESC = 73, 
    DESCENDING = 74, DETACH = 75, DISTINCT = 76, DROP = 77, ELSE = 78, END = 79, 
    ENDS = 80, EXISTS = 81, EXPLAIN = 82, EXPORT = 83, EXTENSION = 84, FALSE = 85, 
    FROM = 86, GLOB = 87, GRAPH = 88, GROUP = 89, HEADERS = 90, HINT = 91, 
    IMPORT = 92, IF = 93, IN = 94, INCREMENT = 95, INSTALL = 96, IS = 97, 
    JOIN = 98, KEY = 99, LIMIT = 100, LOAD = 101, LOGICAL = 102, MACRO = 103, 
    MATCH = 104, MAXVALUE = 105, MERGE = 106, MINVALUE = 107, MULTI_JOIN = 108, 
    NO = 109, NODE = 110, NOT = 111, NULL_ = 112, ON = 113, ONLY = 114, 
    OPTIONAL = 115, OR = 116, ORDER = 117, PRIMARY = 118, PROFILE = 119, 
    PROJECT = 120, RDFGRAPH = 121, READ = 122, REL = 123, RENAME = 124, 
    RETURN = 125, ROLLBACK = 126, ROLLBACK_SKIP_CHECKPOINT = 127, SEQUENCE = 128, 
    SET = 129, SHORTEST = 130, START = 131, STARTS = 132, TABLE = 133, THEN = 134, 
    TO = 135, TRANSACTION = 136, TRUE = 137, TYPE = 138, UNION = 139, UNWIND = 140, 
    USE = 141, WHEN = 142, WHERE = 143, WITH = 144, WRITE = 145, XOR = 146, 
    DECIMAL = 147, STAR = 148, L_SKIP = 149, INVALID_NOT_EQUAL = 150, MINUS = 151, 
    FACTORIAL = 152, COLON = 153, StringLiteral = 154, EscapedChar = 155, 
    DecimalInteger = 156, HexLetter = 157, HexDigit = 158, Digit = 159, 
    NonZeroDigit = 160, NonZeroOctDigit = 161, ZeroDigit = 162, RegularDecimalReal = 163, 
    UnescapedSymbolicName = 164, IdentifierStart = 165, IdentifierPart = 166, 
    EscapedSymbolicName = 167, SP = 168, WHITESPACE = 169, CypherComment = 170, 
    Unknown = 171
  };

  explicit CypherLexer(antlr4::CharStream *input);

  ~CypherLexer() override;


  std::string getGrammarFileName() const override;

  const std::vector<std::string>& getRuleNames() const override;

  const std::vector<std::string>& getChannelNames() const override;

  const std::vector<std::string>& getModeNames() const override;

  const antlr4::dfa::Vocabulary& getVocabulary() const override;

  antlr4::atn::SerializedATNView getSerializedATN() const override;

  const antlr4::atn::ATN& getATN() const override;

  // By default the static state used to implement the lexer is lazily initialized during the first
  // call to the constructor. You can call this function if you wish to initialize the static state
  // ahead of time.
  static void initialize();

private:

  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

};

