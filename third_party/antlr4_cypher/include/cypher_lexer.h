
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
    T__44 = 45, ACYCLIC = 46, ANY = 47, ADD = 48, ALL = 49, ALTER = 50, 
    AND = 51, AS = 52, ASC = 53, ASCENDING = 54, ATTACH = 55, BEGIN = 56, 
    BY = 57, CALL = 58, CASE = 59, CAST = 60, CHECKPOINT = 61, COLUMN = 62, 
    COMMENT = 63, COMMIT = 64, COMMIT_SKIP_CHECKPOINT = 65, CONTAINS = 66, 
    COPY = 67, COUNT = 68, CREATE = 69, CYCLE = 70, DATABASE = 71, DBTYPE = 72, 
    DEFAULT = 73, DELETE = 74, DESC = 75, DESCENDING = 76, DETACH = 77, 
    DISTINCT = 78, DROP = 79, ELSE = 80, END = 81, ENDS = 82, EXISTS = 83, 
    EXPLAIN = 84, EXPORT = 85, EXTENSION = 86, FALSE = 87, FROM = 88, GLOB = 89, 
    GRAPH = 90, GROUP = 91, HEADERS = 92, HINT = 93, IMPORT = 94, IF = 95, 
    IN = 96, INCREMENT = 97, INSTALL = 98, IS = 99, JOIN = 100, KEY = 101, 
    LIMIT = 102, LOAD = 103, LOGICAL = 104, MACRO = 105, MATCH = 106, MAXVALUE = 107, 
    MERGE = 108, MINVALUE = 109, MULTI_JOIN = 110, NO = 111, NODE = 112, 
    NOT = 113, NONE = 114, NULL_ = 115, ON = 116, ONLY = 117, OPTIONAL = 118, 
    OR = 119, ORDER = 120, PRIMARY = 121, PROFILE = 122, PROJECT = 123, 
    READ = 124, REL = 125, RENAME = 126, RETURN = 127, ROLLBACK = 128, ROLLBACK_SKIP_CHECKPOINT = 129, 
    SEQUENCE = 130, SET = 131, SHORTEST = 132, START = 133, STARTS = 134, 
    TABLE = 135, THEN = 136, TO = 137, TRAIL = 138, TRANSACTION = 139, TRUE = 140, 
    TYPE = 141, UNION = 142, UNWIND = 143, USE = 144, WHEN = 145, WHERE = 146, 
    WITH = 147, WRITE = 148, WSHORTEST = 149, XOR = 150, SINGLE = 151, DECIMAL = 152, 
    YIELD = 153, STAR = 154, L_SKIP = 155, INVALID_NOT_EQUAL = 156, MINUS = 157, 
    FACTORIAL = 158, COLON = 159, StringLiteral = 160, EscapedChar = 161, 
    DecimalInteger = 162, HexLetter = 163, HexDigit = 164, Digit = 165, 
    NonZeroDigit = 166, NonZeroOctDigit = 167, ZeroDigit = 168, ExponentDecimalReal = 169, 
    RegularDecimalReal = 170, UnescapedSymbolicName = 171, IdentifierStart = 172, 
    IdentifierPart = 173, EscapedSymbolicName = 174, SP = 175, WHITESPACE = 176, 
    CypherComment = 177, Unknown = 178
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

