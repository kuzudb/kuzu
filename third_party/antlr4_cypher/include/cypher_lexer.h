
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
    T__44 = 45, ATTACH = 46, DBTYPE = 47, USE = 48, CALL = 49, COMMENT_ = 50, 
    MACRO = 51, GLOB = 52, COPY = 53, FROM = 54, COLUMN = 55, EXPORT = 56, 
    IMPORT = 57, DATABASE = 58, IF = 59, NODE = 60, TABLE = 61, GROUP = 62, 
    RDFGRAPH = 63, SEQUENCE = 64, TYPE = 65, INCREMENT = 66, MINVALUE = 67, 
    MAXVALUE = 68, START = 69, NO = 70, CYCLE = 71, DROP = 72, ALTER = 73, 
    DEFAULT = 74, RENAME = 75, ADD = 76, PRIMARY = 77, KEY = 78, REL = 79, 
    TO = 80, DECIMAL = 81, EXPLAIN = 82, PROFILE = 83, BEGIN = 84, TRANSACTION = 85, 
    READ = 86, ONLY = 87, WRITE = 88, COMMIT = 89, COMMIT_SKIP_CHECKPOINT = 90, 
    ROLLBACK = 91, ROLLBACK_SKIP_CHECKPOINT = 92, INSTALL = 93, EXTENSION = 94, 
    UNION = 95, ALL = 96, LOAD = 97, HEADERS = 98, PROJECT = 99, GRAPH = 100, 
    OPTIONAL = 101, MATCH = 102, UNWIND = 103, CREATE = 104, MERGE = 105, 
    ON = 106, SET = 107, DETACH = 108, DELETE = 109, WITH = 110, RETURN = 111, 
    DISTINCT = 112, STAR = 113, AS = 114, ORDER = 115, BY = 116, L_SKIP = 117, 
    LIMIT = 118, ASCENDING = 119, ASC = 120, DESCENDING = 121, DESC = 122, 
    WHERE = 123, SHORTEST = 124, OR = 125, XOR = 126, AND = 127, NOT = 128, 
    INVALID_NOT_EQUAL = 129, MINUS = 130, FACTORIAL = 131, COLON = 132, 
    IN = 133, STARTS = 134, ENDS = 135, CONTAINS = 136, IS = 137, NULL_ = 138, 
    TRUE = 139, FALSE = 140, CAST = 141, COUNT = 142, EXISTS = 143, CASE = 144, 
    ELSE = 145, END = 146, WHEN = 147, THEN = 148, StringLiteral = 149, 
    EscapedChar = 150, DecimalInteger = 151, HexLetter = 152, HexDigit = 153, 
    Digit = 154, NonZeroDigit = 155, NonZeroOctDigit = 156, ZeroDigit = 157, 
    RegularDecimalReal = 158, UnescapedSymbolicName = 159, IdentifierStart = 160, 
    IdentifierPart = 161, EscapedSymbolicName = 162, SP = 163, WHITESPACE = 164, 
    Comment = 165, Unknown = 166
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

