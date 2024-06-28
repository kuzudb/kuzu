
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
    CAST = 58, COLUMN = 59, COMMENT = 60, COMMIT = 61, COMMIT_SKIP_CHECKPOINT = 62, 
    CONTAINS = 63, COPY = 64, COUNT = 65, CREATE = 66, CYCLE = 67, DATABASE = 68, 
    DBTYPE = 69, DEFAULT = 70, DELETE = 71, DESC = 72, DESCENDING = 73, 
    DETACH = 74, DISTINCT = 75, DROP = 76, ELSE = 77, END = 78, ENDS = 79, 
    EXISTS = 80, EXPLAIN = 81, EXPORT = 82, EXTENSION = 83, FALSE = 84, 
    FROM = 85, GLOB = 86, GRAPH = 87, GROUP = 88, HEADERS = 89, HINT = 90, 
    IMPORT = 91, IF = 92, IN = 93, INCREMENT = 94, INSTALL = 95, IS = 96, 
    JOIN = 97, KEY = 98, LIMIT = 99, LOAD = 100, MACRO = 101, MATCH = 102, 
    MAXVALUE = 103, MERGE = 104, MINVALUE = 105, MULTI_JOIN = 106, NO = 107, 
    NODE = 108, NOT = 109, NULL_ = 110, ON = 111, ONLY = 112, OPTIONAL = 113, 
    OR = 114, ORDER = 115, PRIMARY = 116, PROFILE = 117, PROJECT = 118, 
    RDFGRAPH = 119, READ = 120, REL = 121, RENAME = 122, RETURN = 123, ROLLBACK = 124, 
    ROLLBACK_SKIP_CHECKPOINT = 125, SEQUENCE = 126, SET = 127, SHORTEST = 128, 
    START = 129, STARTS = 130, TABLE = 131, THEN = 132, TO = 133, TRANSACTION = 134, 
    TRUE = 135, TYPE = 136, UNION = 137, UNWIND = 138, USE = 139, WHEN = 140, 
    WHERE = 141, WITH = 142, WRITE = 143, XOR = 144, DECIMAL = 145, STAR = 146, 
    L_SKIP = 147, INVALID_NOT_EQUAL = 148, MINUS = 149, FACTORIAL = 150, 
    COLON = 151, StringLiteral = 152, EscapedChar = 153, DecimalInteger = 154, 
    HexLetter = 155, HexDigit = 156, Digit = 157, NonZeroDigit = 158, NonZeroOctDigit = 159, 
    ZeroDigit = 160, RegularDecimalReal = 161, UnescapedSymbolicName = 162, 
    IdentifierStart = 163, IdentifierPart = 164, EscapedSymbolicName = 165, 
    SP = 166, WHITESPACE = 167, CypherComment = 168, Unknown = 169
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

