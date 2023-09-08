
// Generated from Cypher.g4 by ANTLR 4.9

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
    T__44 = 45, T__45 = 46, T__46 = 47, CALL = 48, MACRO = 49, GLOB = 50, 
    COPY = 51, FROM = 52, COLUMN = 53, NODE = 54, TABLE = 55, GROUP = 56, 
    RDF = 57, GRAPH = 58, DROP = 59, ALTER = 60, DEFAULT = 61, RENAME = 62, 
    ADD = 63, PRIMARY = 64, KEY = 65, REL = 66, TO = 67, EXPLAIN = 68, PROFILE = 69, 
    UNION = 70, ALL = 71, OPTIONAL = 72, MATCH = 73, UNWIND = 74, CREATE = 75, 
    MERGE = 76, ON = 77, SET = 78, DELETE = 79, WITH = 80, RETURN = 81, 
    DISTINCT = 82, STAR = 83, AS = 84, ORDER = 85, BY = 86, L_SKIP = 87, 
    LIMIT = 88, ASCENDING = 89, ASC = 90, DESCENDING = 91, DESC = 92, WHERE = 93, 
    SHORTEST = 94, OR = 95, XOR = 96, AND = 97, NOT = 98, INVALID_NOT_EQUAL = 99, 
    MINUS = 100, FACTORIAL = 101, STARTS = 102, ENDS = 103, CONTAINS = 104, 
    IS = 105, NULL_ = 106, TRUE = 107, FALSE = 108, EXISTS = 109, CASE = 110, 
    ELSE = 111, END = 112, WHEN = 113, THEN = 114, StringLiteral = 115, 
    EscapedChar = 116, DecimalInteger = 117, HexLetter = 118, HexDigit = 119, 
    Digit = 120, NonZeroDigit = 121, NonZeroOctDigit = 122, ZeroDigit = 123, 
    RegularDecimalReal = 124, UnescapedSymbolicName = 125, IdentifierStart = 126, 
    IdentifierPart = 127, EscapedSymbolicName = 128, SP = 129, WHITESPACE = 130, 
    Comment = 131, Unknown = 132
  };

  explicit CypherLexer(antlr4::CharStream *input);
  ~CypherLexer();

  virtual std::string getGrammarFileName() const override;
  virtual const std::vector<std::string>& getRuleNames() const override;

  virtual const std::vector<std::string>& getChannelNames() const override;
  virtual const std::vector<std::string>& getModeNames() const override;
  virtual const std::vector<std::string>& getTokenNames() const override; // deprecated, use vocabulary instead
  virtual antlr4::dfa::Vocabulary& getVocabulary() const override;

  virtual const std::vector<uint16_t> getSerializedATN() const override;
  virtual const antlr4::atn::ATN& getATN() const override;

private:
  static std::vector<antlr4::dfa::DFA> _decisionToDFA;
  static antlr4::atn::PredictionContextCache _sharedContextCache;
  static std::vector<std::string> _ruleNames;
  static std::vector<std::string> _tokenNames;
  static std::vector<std::string> _channelNames;
  static std::vector<std::string> _modeNames;

  static std::vector<std::string> _literalNames;
  static std::vector<std::string> _symbolicNames;
  static antlr4::dfa::Vocabulary _vocabulary;
  static antlr4::atn::ATN _atn;
  static std::vector<uint16_t> _serializedATN;


  // Individual action functions triggered by action() above.

  // Individual semantic predicate functions triggered by sempred() above.

  struct Initializer {
    Initializer();
  };
  static Initializer _init;
};

