
// Generated from Cypher.g4 by ANTLR 4.9



#include "cypher_parser.h"


using namespace antlrcpp;
using namespace antlr4;

CypherParser::CypherParser(TokenStream *input) : Parser(input) {
  _interpreter = new atn::ParserATNSimulator(this, _atn, _decisionToDFA, _sharedContextCache);
}

CypherParser::~CypherParser() {
  delete _interpreter;
}

std::string CypherParser::getGrammarFileName() const {
  return "Cypher.g4";
}

const std::vector<std::string>& CypherParser::getRuleNames() const {
  return _ruleNames;
}

dfa::Vocabulary& CypherParser::getVocabulary() const {
  return _vocabulary;
}


//----------------- OC_CypherContext ------------------------------------------------------------------

CypherParser::OC_CypherContext::OC_CypherContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_CypherContext::EOF() {
  return getToken(CypherParser::EOF, 0);
}

CypherParser::OC_StatementContext* CypherParser::OC_CypherContext::oC_Statement() {
  return getRuleContext<CypherParser::OC_StatementContext>(0);
}

CypherParser::KU_DDLContext* CypherParser::OC_CypherContext::kU_DDL() {
  return getRuleContext<CypherParser::KU_DDLContext>(0);
}

CypherParser::KU_CopyCSVContext* CypherParser::OC_CypherContext::kU_CopyCSV() {
  return getRuleContext<CypherParser::KU_CopyCSVContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_CypherContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_CypherContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_AnyCypherOptionContext* CypherParser::OC_CypherContext::oC_AnyCypherOption() {
  return getRuleContext<CypherParser::OC_AnyCypherOptionContext>(0);
}


size_t CypherParser::OC_CypherContext::getRuleIndex() const {
  return CypherParser::RuleOC_Cypher;
}


CypherParser::OC_CypherContext* CypherParser::oC_Cypher() {
  OC_CypherContext *_localctx = _tracker.createInstance<OC_CypherContext>(_ctx, getState());
  enterRule(_localctx, 0, CypherParser::RuleOC_Cypher);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(229);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 0, _ctx)) {
    case 1: {
      setState(228);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(232);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::EXPLAIN

    || _la == CypherParser::PROFILE) {
      setState(231);
      oC_AnyCypherOption();
    }
    setState(235);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 2, _ctx)) {
    case 1: {
      setState(234);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(240);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 3, _ctx)) {
    case 1: {
      setState(237);
      oC_Statement();
      break;
    }

    case 2: {
      setState(238);
      kU_DDL();
      break;
    }

    case 3: {
      setState(239);
      kU_CopyCSV();
      break;
    }

    default:
      break;
    }
    setState(246);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 5, _ctx)) {
    case 1: {
      setState(243);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(242);
        match(CypherParser::SP);
      }
      setState(245);
      match(CypherParser::T__0);
      break;
    }

    default:
      break;
    }
    setState(249);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(248);
      match(CypherParser::SP);
    }
    setState(251);
    match(CypherParser::EOF);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_CopyCSVContext ------------------------------------------------------------------

CypherParser::KU_CopyCSVContext::KU_CopyCSVContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_CopyCSVContext::COPY() {
  return getToken(CypherParser::COPY, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_CopyCSVContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_CopyCSVContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_CopyCSVContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}

tree::TerminalNode* CypherParser::KU_CopyCSVContext::FROM() {
  return getToken(CypherParser::FROM, 0);
}

tree::TerminalNode* CypherParser::KU_CopyCSVContext::StringLiteral() {
  return getToken(CypherParser::StringLiteral, 0);
}

CypherParser::KU_ParsingOptionsContext* CypherParser::KU_CopyCSVContext::kU_ParsingOptions() {
  return getRuleContext<CypherParser::KU_ParsingOptionsContext>(0);
}


size_t CypherParser::KU_CopyCSVContext::getRuleIndex() const {
  return CypherParser::RuleKU_CopyCSV;
}


CypherParser::KU_CopyCSVContext* CypherParser::kU_CopyCSV() {
  KU_CopyCSVContext *_localctx = _tracker.createInstance<KU_CopyCSVContext>(_ctx, getState());
  enterRule(_localctx, 2, CypherParser::RuleKU_CopyCSV);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(253);
    match(CypherParser::COPY);
    setState(254);
    match(CypherParser::SP);
    setState(255);
    oC_SchemaName();
    setState(256);
    match(CypherParser::SP);
    setState(257);
    match(CypherParser::FROM);
    setState(258);
    match(CypherParser::SP);
    setState(259);
    match(CypherParser::StringLiteral);
    setState(273);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 10, _ctx)) {
    case 1: {
      setState(261);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(260);
        match(CypherParser::SP);
      }
      setState(263);
      match(CypherParser::T__1);
      setState(265);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(264);
        match(CypherParser::SP);
      }
      setState(267);
      kU_ParsingOptions();
      setState(269);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(268);
        match(CypherParser::SP);
      }
      setState(271);
      match(CypherParser::T__2);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ParsingOptionsContext ------------------------------------------------------------------

CypherParser::KU_ParsingOptionsContext::KU_ParsingOptionsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_ParsingOptionContext *> CypherParser::KU_ParsingOptionsContext::kU_ParsingOption() {
  return getRuleContexts<CypherParser::KU_ParsingOptionContext>();
}

CypherParser::KU_ParsingOptionContext* CypherParser::KU_ParsingOptionsContext::kU_ParsingOption(size_t i) {
  return getRuleContext<CypherParser::KU_ParsingOptionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_ParsingOptionsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_ParsingOptionsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_ParsingOptionsContext::getRuleIndex() const {
  return CypherParser::RuleKU_ParsingOptions;
}


CypherParser::KU_ParsingOptionsContext* CypherParser::kU_ParsingOptions() {
  KU_ParsingOptionsContext *_localctx = _tracker.createInstance<KU_ParsingOptionsContext>(_ctx, getState());
  enterRule(_localctx, 4, CypherParser::RuleKU_ParsingOptions);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(275);
    kU_ParsingOption();
    setState(286);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 13, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(277);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(276);
          match(CypherParser::SP);
        }
        setState(279);
        match(CypherParser::T__3);
        setState(281);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(280);
          match(CypherParser::SP);
        }
        setState(283);
        kU_ParsingOption(); 
      }
      setState(288);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 13, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ParsingOptionContext ------------------------------------------------------------------

CypherParser::KU_ParsingOptionContext::KU_ParsingOptionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::KU_ParsingOptionContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}

CypherParser::OC_LiteralContext* CypherParser::KU_ParsingOptionContext::oC_Literal() {
  return getRuleContext<CypherParser::OC_LiteralContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_ParsingOptionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_ParsingOptionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_ParsingOptionContext::getRuleIndex() const {
  return CypherParser::RuleKU_ParsingOption;
}


CypherParser::KU_ParsingOptionContext* CypherParser::kU_ParsingOption() {
  KU_ParsingOptionContext *_localctx = _tracker.createInstance<KU_ParsingOptionContext>(_ctx, getState());
  enterRule(_localctx, 6, CypherParser::RuleKU_ParsingOption);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(289);
    oC_SymbolicName();
    setState(291);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(290);
      match(CypherParser::SP);
    }
    setState(293);
    match(CypherParser::T__4);
    setState(295);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(294);
      match(CypherParser::SP);
    }
    setState(297);
    oC_Literal();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_DDLContext ------------------------------------------------------------------

CypherParser::KU_DDLContext::KU_DDLContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::KU_CreateNodeContext* CypherParser::KU_DDLContext::kU_CreateNode() {
  return getRuleContext<CypherParser::KU_CreateNodeContext>(0);
}

CypherParser::KU_CreateRelContext* CypherParser::KU_DDLContext::kU_CreateRel() {
  return getRuleContext<CypherParser::KU_CreateRelContext>(0);
}

CypherParser::KU_DropTableContext* CypherParser::KU_DDLContext::kU_DropTable() {
  return getRuleContext<CypherParser::KU_DropTableContext>(0);
}

CypherParser::KU_AlterTableContext* CypherParser::KU_DDLContext::kU_AlterTable() {
  return getRuleContext<CypherParser::KU_AlterTableContext>(0);
}


size_t CypherParser::KU_DDLContext::getRuleIndex() const {
  return CypherParser::RuleKU_DDL;
}


CypherParser::KU_DDLContext* CypherParser::kU_DDL() {
  KU_DDLContext *_localctx = _tracker.createInstance<KU_DDLContext>(_ctx, getState());
  enterRule(_localctx, 8, CypherParser::RuleKU_DDL);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(303);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 16, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(299);
      kU_CreateNode();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(300);
      kU_CreateRel();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(301);
      kU_DropTable();
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(302);
      kU_AlterTable();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_CreateNodeContext ------------------------------------------------------------------

CypherParser::KU_CreateNodeContext::KU_CreateNodeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_CreateNodeContext::CREATE() {
  return getToken(CypherParser::CREATE, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_CreateNodeContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_CreateNodeContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_CreateNodeContext::NODE() {
  return getToken(CypherParser::NODE, 0);
}

tree::TerminalNode* CypherParser::KU_CreateNodeContext::TABLE() {
  return getToken(CypherParser::TABLE, 0);
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_CreateNodeContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}

CypherParser::KU_PropertyDefinitionsContext* CypherParser::KU_CreateNodeContext::kU_PropertyDefinitions() {
  return getRuleContext<CypherParser::KU_PropertyDefinitionsContext>(0);
}

CypherParser::KU_CreateNodeConstraintContext* CypherParser::KU_CreateNodeContext::kU_CreateNodeConstraint() {
  return getRuleContext<CypherParser::KU_CreateNodeConstraintContext>(0);
}


size_t CypherParser::KU_CreateNodeContext::getRuleIndex() const {
  return CypherParser::RuleKU_CreateNode;
}


CypherParser::KU_CreateNodeContext* CypherParser::kU_CreateNode() {
  KU_CreateNodeContext *_localctx = _tracker.createInstance<KU_CreateNodeContext>(_ctx, getState());
  enterRule(_localctx, 10, CypherParser::RuleKU_CreateNode);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(305);
    match(CypherParser::CREATE);
    setState(306);
    match(CypherParser::SP);
    setState(307);
    match(CypherParser::NODE);
    setState(308);
    match(CypherParser::SP);
    setState(309);
    match(CypherParser::TABLE);
    setState(310);
    match(CypherParser::SP);
    setState(311);
    oC_SchemaName();
    setState(313);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(312);
      match(CypherParser::SP);
    }
    setState(315);
    match(CypherParser::T__1);
    setState(317);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(316);
      match(CypherParser::SP);
    }
    setState(319);
    kU_PropertyDefinitions();
    setState(321);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(320);
      match(CypherParser::SP);
    }

    setState(323);
    match(CypherParser::T__3);
    setState(325);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(324);
      match(CypherParser::SP);
    }
    setState(327);
    kU_CreateNodeConstraint();
    setState(330);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(329);
      match(CypherParser::SP);
    }
    setState(332);
    match(CypherParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_CreateRelContext ------------------------------------------------------------------

CypherParser::KU_CreateRelContext::KU_CreateRelContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_CreateRelContext::CREATE() {
  return getToken(CypherParser::CREATE, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_CreateRelContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_CreateRelContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_CreateRelContext::REL() {
  return getToken(CypherParser::REL, 0);
}

tree::TerminalNode* CypherParser::KU_CreateRelContext::TABLE() {
  return getToken(CypherParser::TABLE, 0);
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_CreateRelContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}

CypherParser::KU_RelConnectionsContext* CypherParser::KU_CreateRelContext::kU_RelConnections() {
  return getRuleContext<CypherParser::KU_RelConnectionsContext>(0);
}

CypherParser::KU_PropertyDefinitionsContext* CypherParser::KU_CreateRelContext::kU_PropertyDefinitions() {
  return getRuleContext<CypherParser::KU_PropertyDefinitionsContext>(0);
}

CypherParser::OC_SymbolicNameContext* CypherParser::KU_CreateRelContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}


size_t CypherParser::KU_CreateRelContext::getRuleIndex() const {
  return CypherParser::RuleKU_CreateRel;
}


CypherParser::KU_CreateRelContext* CypherParser::kU_CreateRel() {
  KU_CreateRelContext *_localctx = _tracker.createInstance<KU_CreateRelContext>(_ctx, getState());
  enterRule(_localctx, 12, CypherParser::RuleKU_CreateRel);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(334);
    match(CypherParser::CREATE);
    setState(335);
    match(CypherParser::SP);
    setState(336);
    match(CypherParser::REL);
    setState(337);
    match(CypherParser::SP);
    setState(338);
    match(CypherParser::TABLE);
    setState(339);
    match(CypherParser::SP);
    setState(340);
    oC_SchemaName();
    setState(342);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(341);
      match(CypherParser::SP);
    }
    setState(344);
    match(CypherParser::T__1);
    setState(346);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(345);
      match(CypherParser::SP);
    }
    setState(348);
    kU_RelConnections();
    setState(350);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(349);
      match(CypherParser::SP);
    }
    setState(360);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 27, _ctx)) {
    case 1: {
      setState(352);
      match(CypherParser::T__3);
      setState(354);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(353);
        match(CypherParser::SP);
      }
      setState(356);
      kU_PropertyDefinitions();
      setState(358);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(357);
        match(CypherParser::SP);
      }
      break;
    }

    default:
      break;
    }
    setState(370);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__3) {
      setState(362);
      match(CypherParser::T__3);
      setState(364);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(363);
        match(CypherParser::SP);
      }
      setState(366);
      oC_SymbolicName();
      setState(368);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(367);
        match(CypherParser::SP);
      }
    }
    setState(372);
    match(CypherParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_DropTableContext ------------------------------------------------------------------

CypherParser::KU_DropTableContext::KU_DropTableContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_DropTableContext::DROP() {
  return getToken(CypherParser::DROP, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_DropTableContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_DropTableContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_DropTableContext::TABLE() {
  return getToken(CypherParser::TABLE, 0);
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_DropTableContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}


size_t CypherParser::KU_DropTableContext::getRuleIndex() const {
  return CypherParser::RuleKU_DropTable;
}


CypherParser::KU_DropTableContext* CypherParser::kU_DropTable() {
  KU_DropTableContext *_localctx = _tracker.createInstance<KU_DropTableContext>(_ctx, getState());
  enterRule(_localctx, 14, CypherParser::RuleKU_DropTable);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(374);
    match(CypherParser::DROP);
    setState(375);
    match(CypherParser::SP);
    setState(376);
    match(CypherParser::TABLE);
    setState(377);
    match(CypherParser::SP);
    setState(378);
    oC_SchemaName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_AlterTableContext ------------------------------------------------------------------

CypherParser::KU_AlterTableContext::KU_AlterTableContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_AlterTableContext::ALTER() {
  return getToken(CypherParser::ALTER, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_AlterTableContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_AlterTableContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_AlterTableContext::TABLE() {
  return getToken(CypherParser::TABLE, 0);
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_AlterTableContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}

CypherParser::KU_AlterOptionsContext* CypherParser::KU_AlterTableContext::kU_AlterOptions() {
  return getRuleContext<CypherParser::KU_AlterOptionsContext>(0);
}


size_t CypherParser::KU_AlterTableContext::getRuleIndex() const {
  return CypherParser::RuleKU_AlterTable;
}


CypherParser::KU_AlterTableContext* CypherParser::kU_AlterTable() {
  KU_AlterTableContext *_localctx = _tracker.createInstance<KU_AlterTableContext>(_ctx, getState());
  enterRule(_localctx, 16, CypherParser::RuleKU_AlterTable);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(380);
    match(CypherParser::ALTER);
    setState(381);
    match(CypherParser::SP);
    setState(382);
    match(CypherParser::TABLE);
    setState(383);
    match(CypherParser::SP);
    setState(384);
    oC_SchemaName();
    setState(385);
    match(CypherParser::SP);
    setState(386);
    kU_AlterOptions();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_AlterOptionsContext ------------------------------------------------------------------

CypherParser::KU_AlterOptionsContext::KU_AlterOptionsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::KU_AddPropertyContext* CypherParser::KU_AlterOptionsContext::kU_AddProperty() {
  return getRuleContext<CypherParser::KU_AddPropertyContext>(0);
}

CypherParser::KU_DropPropertyContext* CypherParser::KU_AlterOptionsContext::kU_DropProperty() {
  return getRuleContext<CypherParser::KU_DropPropertyContext>(0);
}

CypherParser::KU_RenameTableContext* CypherParser::KU_AlterOptionsContext::kU_RenameTable() {
  return getRuleContext<CypherParser::KU_RenameTableContext>(0);
}

CypherParser::KU_RenamePropertyContext* CypherParser::KU_AlterOptionsContext::kU_RenameProperty() {
  return getRuleContext<CypherParser::KU_RenamePropertyContext>(0);
}


size_t CypherParser::KU_AlterOptionsContext::getRuleIndex() const {
  return CypherParser::RuleKU_AlterOptions;
}


CypherParser::KU_AlterOptionsContext* CypherParser::kU_AlterOptions() {
  KU_AlterOptionsContext *_localctx = _tracker.createInstance<KU_AlterOptionsContext>(_ctx, getState());
  enterRule(_localctx, 18, CypherParser::RuleKU_AlterOptions);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(392);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 31, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(388);
      kU_AddProperty();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(389);
      kU_DropProperty();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(390);
      kU_RenameTable();
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(391);
      kU_RenameProperty();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_AddPropertyContext ------------------------------------------------------------------

CypherParser::KU_AddPropertyContext::KU_AddPropertyContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_AddPropertyContext::ADD() {
  return getToken(CypherParser::ADD, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_AddPropertyContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_AddPropertyContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::KU_AddPropertyContext::oC_PropertyKeyName() {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(0);
}

CypherParser::KU_DataTypeContext* CypherParser::KU_AddPropertyContext::kU_DataType() {
  return getRuleContext<CypherParser::KU_DataTypeContext>(0);
}

tree::TerminalNode* CypherParser::KU_AddPropertyContext::DEFAULT() {
  return getToken(CypherParser::DEFAULT, 0);
}

CypherParser::OC_ExpressionContext* CypherParser::KU_AddPropertyContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}


size_t CypherParser::KU_AddPropertyContext::getRuleIndex() const {
  return CypherParser::RuleKU_AddProperty;
}


CypherParser::KU_AddPropertyContext* CypherParser::kU_AddProperty() {
  KU_AddPropertyContext *_localctx = _tracker.createInstance<KU_AddPropertyContext>(_ctx, getState());
  enterRule(_localctx, 20, CypherParser::RuleKU_AddProperty);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(394);
    match(CypherParser::ADD);
    setState(395);
    match(CypherParser::SP);
    setState(396);
    oC_PropertyKeyName();
    setState(397);
    match(CypherParser::SP);
    setState(398);
    kU_DataType();
    setState(403);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 32, _ctx)) {
    case 1: {
      setState(399);
      match(CypherParser::SP);
      setState(400);
      match(CypherParser::DEFAULT);
      setState(401);
      match(CypherParser::SP);
      setState(402);
      oC_Expression();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_DropPropertyContext ------------------------------------------------------------------

CypherParser::KU_DropPropertyContext::KU_DropPropertyContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_DropPropertyContext::DROP() {
  return getToken(CypherParser::DROP, 0);
}

tree::TerminalNode* CypherParser::KU_DropPropertyContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::KU_DropPropertyContext::oC_PropertyKeyName() {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(0);
}


size_t CypherParser::KU_DropPropertyContext::getRuleIndex() const {
  return CypherParser::RuleKU_DropProperty;
}


CypherParser::KU_DropPropertyContext* CypherParser::kU_DropProperty() {
  KU_DropPropertyContext *_localctx = _tracker.createInstance<KU_DropPropertyContext>(_ctx, getState());
  enterRule(_localctx, 22, CypherParser::RuleKU_DropProperty);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(405);
    match(CypherParser::DROP);
    setState(406);
    match(CypherParser::SP);
    setState(407);
    oC_PropertyKeyName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_RenameTableContext ------------------------------------------------------------------

CypherParser::KU_RenameTableContext::KU_RenameTableContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_RenameTableContext::RENAME() {
  return getToken(CypherParser::RENAME, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_RenameTableContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_RenameTableContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_RenameTableContext::TO() {
  return getToken(CypherParser::TO, 0);
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_RenameTableContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}


size_t CypherParser::KU_RenameTableContext::getRuleIndex() const {
  return CypherParser::RuleKU_RenameTable;
}


CypherParser::KU_RenameTableContext* CypherParser::kU_RenameTable() {
  KU_RenameTableContext *_localctx = _tracker.createInstance<KU_RenameTableContext>(_ctx, getState());
  enterRule(_localctx, 24, CypherParser::RuleKU_RenameTable);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(409);
    match(CypherParser::RENAME);
    setState(410);
    match(CypherParser::SP);
    setState(411);
    match(CypherParser::TO);
    setState(412);
    match(CypherParser::SP);
    setState(413);
    oC_SchemaName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_RenamePropertyContext ------------------------------------------------------------------

CypherParser::KU_RenamePropertyContext::KU_RenamePropertyContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_RenamePropertyContext::RENAME() {
  return getToken(CypherParser::RENAME, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_RenamePropertyContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_RenamePropertyContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::OC_PropertyKeyNameContext *> CypherParser::KU_RenamePropertyContext::oC_PropertyKeyName() {
  return getRuleContexts<CypherParser::OC_PropertyKeyNameContext>();
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::KU_RenamePropertyContext::oC_PropertyKeyName(size_t i) {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(i);
}

tree::TerminalNode* CypherParser::KU_RenamePropertyContext::TO() {
  return getToken(CypherParser::TO, 0);
}


size_t CypherParser::KU_RenamePropertyContext::getRuleIndex() const {
  return CypherParser::RuleKU_RenameProperty;
}


CypherParser::KU_RenamePropertyContext* CypherParser::kU_RenameProperty() {
  KU_RenamePropertyContext *_localctx = _tracker.createInstance<KU_RenamePropertyContext>(_ctx, getState());
  enterRule(_localctx, 26, CypherParser::RuleKU_RenameProperty);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(415);
    match(CypherParser::RENAME);
    setState(416);
    match(CypherParser::SP);
    setState(417);
    oC_PropertyKeyName();
    setState(418);
    match(CypherParser::SP);
    setState(419);
    match(CypherParser::TO);
    setState(420);
    match(CypherParser::SP);
    setState(421);
    oC_PropertyKeyName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_RelConnectionsContext ------------------------------------------------------------------

CypherParser::KU_RelConnectionsContext::KU_RelConnectionsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_RelConnectionContext *> CypherParser::KU_RelConnectionsContext::kU_RelConnection() {
  return getRuleContexts<CypherParser::KU_RelConnectionContext>();
}

CypherParser::KU_RelConnectionContext* CypherParser::KU_RelConnectionsContext::kU_RelConnection(size_t i) {
  return getRuleContext<CypherParser::KU_RelConnectionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_RelConnectionsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_RelConnectionsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_RelConnectionsContext::getRuleIndex() const {
  return CypherParser::RuleKU_RelConnections;
}


CypherParser::KU_RelConnectionsContext* CypherParser::kU_RelConnections() {
  KU_RelConnectionsContext *_localctx = _tracker.createInstance<KU_RelConnectionsContext>(_ctx, getState());
  enterRule(_localctx, 28, CypherParser::RuleKU_RelConnections);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(423);
    kU_RelConnection();
    setState(434);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 35, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(425);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(424);
          match(CypherParser::SP);
        }
        setState(427);
        match(CypherParser::T__3);
        setState(429);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(428);
          match(CypherParser::SP);
        }
        setState(431);
        kU_RelConnection(); 
      }
      setState(436);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 35, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_RelConnectionContext ------------------------------------------------------------------

CypherParser::KU_RelConnectionContext::KU_RelConnectionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_RelConnectionContext::FROM() {
  return getToken(CypherParser::FROM, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_RelConnectionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_RelConnectionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::KU_NodeLabelsContext *> CypherParser::KU_RelConnectionContext::kU_NodeLabels() {
  return getRuleContexts<CypherParser::KU_NodeLabelsContext>();
}

CypherParser::KU_NodeLabelsContext* CypherParser::KU_RelConnectionContext::kU_NodeLabels(size_t i) {
  return getRuleContext<CypherParser::KU_NodeLabelsContext>(i);
}

tree::TerminalNode* CypherParser::KU_RelConnectionContext::TO() {
  return getToken(CypherParser::TO, 0);
}


size_t CypherParser::KU_RelConnectionContext::getRuleIndex() const {
  return CypherParser::RuleKU_RelConnection;
}


CypherParser::KU_RelConnectionContext* CypherParser::kU_RelConnection() {
  KU_RelConnectionContext *_localctx = _tracker.createInstance<KU_RelConnectionContext>(_ctx, getState());
  enterRule(_localctx, 30, CypherParser::RuleKU_RelConnection);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(437);
    match(CypherParser::FROM);
    setState(438);
    match(CypherParser::SP);
    setState(439);
    kU_NodeLabels();
    setState(440);
    match(CypherParser::SP);
    setState(441);
    match(CypherParser::TO);
    setState(442);
    match(CypherParser::SP);
    setState(443);
    kU_NodeLabels();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_NodeLabelsContext ------------------------------------------------------------------

CypherParser::KU_NodeLabelsContext::KU_NodeLabelsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_SchemaNameContext *> CypherParser::KU_NodeLabelsContext::oC_SchemaName() {
  return getRuleContexts<CypherParser::OC_SchemaNameContext>();
}

CypherParser::OC_SchemaNameContext* CypherParser::KU_NodeLabelsContext::oC_SchemaName(size_t i) {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_NodeLabelsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_NodeLabelsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_NodeLabelsContext::getRuleIndex() const {
  return CypherParser::RuleKU_NodeLabels;
}


CypherParser::KU_NodeLabelsContext* CypherParser::kU_NodeLabels() {
  KU_NodeLabelsContext *_localctx = _tracker.createInstance<KU_NodeLabelsContext>(_ctx, getState());
  enterRule(_localctx, 32, CypherParser::RuleKU_NodeLabels);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(445);
    oC_SchemaName();
    setState(456);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 38, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(447);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(446);
          match(CypherParser::SP);
        }
        setState(449);
        match(CypherParser::T__5);
        setState(451);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(450);
          match(CypherParser::SP);
        }
        setState(453);
        oC_SchemaName(); 
      }
      setState(458);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 38, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_PropertyDefinitionsContext ------------------------------------------------------------------

CypherParser::KU_PropertyDefinitionsContext::KU_PropertyDefinitionsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_PropertyDefinitionContext *> CypherParser::KU_PropertyDefinitionsContext::kU_PropertyDefinition() {
  return getRuleContexts<CypherParser::KU_PropertyDefinitionContext>();
}

CypherParser::KU_PropertyDefinitionContext* CypherParser::KU_PropertyDefinitionsContext::kU_PropertyDefinition(size_t i) {
  return getRuleContext<CypherParser::KU_PropertyDefinitionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_PropertyDefinitionsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_PropertyDefinitionsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_PropertyDefinitionsContext::getRuleIndex() const {
  return CypherParser::RuleKU_PropertyDefinitions;
}


CypherParser::KU_PropertyDefinitionsContext* CypherParser::kU_PropertyDefinitions() {
  KU_PropertyDefinitionsContext *_localctx = _tracker.createInstance<KU_PropertyDefinitionsContext>(_ctx, getState());
  enterRule(_localctx, 34, CypherParser::RuleKU_PropertyDefinitions);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(459);
    kU_PropertyDefinition();
    setState(470);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 41, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(461);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(460);
          match(CypherParser::SP);
        }
        setState(463);
        match(CypherParser::T__3);
        setState(465);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(464);
          match(CypherParser::SP);
        }
        setState(467);
        kU_PropertyDefinition(); 
      }
      setState(472);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 41, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_PropertyDefinitionContext ------------------------------------------------------------------

CypherParser::KU_PropertyDefinitionContext::KU_PropertyDefinitionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::KU_PropertyDefinitionContext::oC_PropertyKeyName() {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(0);
}

tree::TerminalNode* CypherParser::KU_PropertyDefinitionContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::KU_DataTypeContext* CypherParser::KU_PropertyDefinitionContext::kU_DataType() {
  return getRuleContext<CypherParser::KU_DataTypeContext>(0);
}


size_t CypherParser::KU_PropertyDefinitionContext::getRuleIndex() const {
  return CypherParser::RuleKU_PropertyDefinition;
}


CypherParser::KU_PropertyDefinitionContext* CypherParser::kU_PropertyDefinition() {
  KU_PropertyDefinitionContext *_localctx = _tracker.createInstance<KU_PropertyDefinitionContext>(_ctx, getState());
  enterRule(_localctx, 36, CypherParser::RuleKU_PropertyDefinition);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(473);
    oC_PropertyKeyName();
    setState(474);
    match(CypherParser::SP);
    setState(475);
    kU_DataType();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_CreateNodeConstraintContext ------------------------------------------------------------------

CypherParser::KU_CreateNodeConstraintContext::KU_CreateNodeConstraintContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_CreateNodeConstraintContext::PRIMARY() {
  return getToken(CypherParser::PRIMARY, 0);
}

std::vector<tree::TerminalNode *> CypherParser::KU_CreateNodeConstraintContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_CreateNodeConstraintContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::KU_CreateNodeConstraintContext::KEY() {
  return getToken(CypherParser::KEY, 0);
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::KU_CreateNodeConstraintContext::oC_PropertyKeyName() {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(0);
}


size_t CypherParser::KU_CreateNodeConstraintContext::getRuleIndex() const {
  return CypherParser::RuleKU_CreateNodeConstraint;
}


CypherParser::KU_CreateNodeConstraintContext* CypherParser::kU_CreateNodeConstraint() {
  KU_CreateNodeConstraintContext *_localctx = _tracker.createInstance<KU_CreateNodeConstraintContext>(_ctx, getState());
  enterRule(_localctx, 38, CypherParser::RuleKU_CreateNodeConstraint);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(477);
    match(CypherParser::PRIMARY);
    setState(478);
    match(CypherParser::SP);
    setState(479);
    match(CypherParser::KEY);
    setState(481);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(480);
      match(CypherParser::SP);
    }
    setState(483);
    match(CypherParser::T__1);
    setState(485);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(484);
      match(CypherParser::SP);
    }
    setState(487);
    oC_PropertyKeyName();
    setState(489);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(488);
      match(CypherParser::SP);
    }
    setState(491);
    match(CypherParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_DataTypeContext ------------------------------------------------------------------

CypherParser::KU_DataTypeContext::KU_DataTypeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::KU_DataTypeContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}

CypherParser::KU_ListIdentifiersContext* CypherParser::KU_DataTypeContext::kU_ListIdentifiers() {
  return getRuleContext<CypherParser::KU_ListIdentifiersContext>(0);
}


size_t CypherParser::KU_DataTypeContext::getRuleIndex() const {
  return CypherParser::RuleKU_DataType;
}


CypherParser::KU_DataTypeContext* CypherParser::kU_DataType() {
  KU_DataTypeContext *_localctx = _tracker.createInstance<KU_DataTypeContext>(_ctx, getState());
  enterRule(_localctx, 40, CypherParser::RuleKU_DataType);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(497);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 45, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(493);
      oC_SymbolicName();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(494);
      oC_SymbolicName();
      setState(495);
      kU_ListIdentifiers();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ListIdentifiersContext ------------------------------------------------------------------

CypherParser::KU_ListIdentifiersContext::KU_ListIdentifiersContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_ListIdentifierContext *> CypherParser::KU_ListIdentifiersContext::kU_ListIdentifier() {
  return getRuleContexts<CypherParser::KU_ListIdentifierContext>();
}

CypherParser::KU_ListIdentifierContext* CypherParser::KU_ListIdentifiersContext::kU_ListIdentifier(size_t i) {
  return getRuleContext<CypherParser::KU_ListIdentifierContext>(i);
}


size_t CypherParser::KU_ListIdentifiersContext::getRuleIndex() const {
  return CypherParser::RuleKU_ListIdentifiers;
}


CypherParser::KU_ListIdentifiersContext* CypherParser::kU_ListIdentifiers() {
  KU_ListIdentifiersContext *_localctx = _tracker.createInstance<KU_ListIdentifiersContext>(_ctx, getState());
  enterRule(_localctx, 42, CypherParser::RuleKU_ListIdentifiers);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(499);
    kU_ListIdentifier();
    setState(503);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == CypherParser::T__6) {
      setState(500);
      kU_ListIdentifier();
      setState(505);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ListIdentifierContext ------------------------------------------------------------------

CypherParser::KU_ListIdentifierContext::KU_ListIdentifierContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t CypherParser::KU_ListIdentifierContext::getRuleIndex() const {
  return CypherParser::RuleKU_ListIdentifier;
}


CypherParser::KU_ListIdentifierContext* CypherParser::kU_ListIdentifier() {
  KU_ListIdentifierContext *_localctx = _tracker.createInstance<KU_ListIdentifierContext>(_ctx, getState());
  enterRule(_localctx, 44, CypherParser::RuleKU_ListIdentifier);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(506);
    match(CypherParser::T__6);
    setState(507);
    match(CypherParser::T__7);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_AnyCypherOptionContext ------------------------------------------------------------------

CypherParser::OC_AnyCypherOptionContext::OC_AnyCypherOptionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ExplainContext* CypherParser::OC_AnyCypherOptionContext::oC_Explain() {
  return getRuleContext<CypherParser::OC_ExplainContext>(0);
}

CypherParser::OC_ProfileContext* CypherParser::OC_AnyCypherOptionContext::oC_Profile() {
  return getRuleContext<CypherParser::OC_ProfileContext>(0);
}


size_t CypherParser::OC_AnyCypherOptionContext::getRuleIndex() const {
  return CypherParser::RuleOC_AnyCypherOption;
}


CypherParser::OC_AnyCypherOptionContext* CypherParser::oC_AnyCypherOption() {
  OC_AnyCypherOptionContext *_localctx = _tracker.createInstance<OC_AnyCypherOptionContext>(_ctx, getState());
  enterRule(_localctx, 46, CypherParser::RuleOC_AnyCypherOption);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(511);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::EXPLAIN: {
        enterOuterAlt(_localctx, 1);
        setState(509);
        oC_Explain();
        break;
      }

      case CypherParser::PROFILE: {
        enterOuterAlt(_localctx, 2);
        setState(510);
        oC_Profile();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ExplainContext ------------------------------------------------------------------

CypherParser::OC_ExplainContext::OC_ExplainContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_ExplainContext::EXPLAIN() {
  return getToken(CypherParser::EXPLAIN, 0);
}


size_t CypherParser::OC_ExplainContext::getRuleIndex() const {
  return CypherParser::RuleOC_Explain;
}


CypherParser::OC_ExplainContext* CypherParser::oC_Explain() {
  OC_ExplainContext *_localctx = _tracker.createInstance<OC_ExplainContext>(_ctx, getState());
  enterRule(_localctx, 48, CypherParser::RuleOC_Explain);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(513);
    match(CypherParser::EXPLAIN);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ProfileContext ------------------------------------------------------------------

CypherParser::OC_ProfileContext::OC_ProfileContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_ProfileContext::PROFILE() {
  return getToken(CypherParser::PROFILE, 0);
}


size_t CypherParser::OC_ProfileContext::getRuleIndex() const {
  return CypherParser::RuleOC_Profile;
}


CypherParser::OC_ProfileContext* CypherParser::oC_Profile() {
  OC_ProfileContext *_localctx = _tracker.createInstance<OC_ProfileContext>(_ctx, getState());
  enterRule(_localctx, 50, CypherParser::RuleOC_Profile);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(515);
    match(CypherParser::PROFILE);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_StatementContext ------------------------------------------------------------------

CypherParser::OC_StatementContext::OC_StatementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_QueryContext* CypherParser::OC_StatementContext::oC_Query() {
  return getRuleContext<CypherParser::OC_QueryContext>(0);
}


size_t CypherParser::OC_StatementContext::getRuleIndex() const {
  return CypherParser::RuleOC_Statement;
}


CypherParser::OC_StatementContext* CypherParser::oC_Statement() {
  OC_StatementContext *_localctx = _tracker.createInstance<OC_StatementContext>(_ctx, getState());
  enterRule(_localctx, 52, CypherParser::RuleOC_Statement);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(517);
    oC_Query();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_QueryContext ------------------------------------------------------------------

CypherParser::OC_QueryContext::OC_QueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_RegularQueryContext* CypherParser::OC_QueryContext::oC_RegularQuery() {
  return getRuleContext<CypherParser::OC_RegularQueryContext>(0);
}


size_t CypherParser::OC_QueryContext::getRuleIndex() const {
  return CypherParser::RuleOC_Query;
}


CypherParser::OC_QueryContext* CypherParser::oC_Query() {
  OC_QueryContext *_localctx = _tracker.createInstance<OC_QueryContext>(_ctx, getState());
  enterRule(_localctx, 54, CypherParser::RuleOC_Query);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(519);
    oC_RegularQuery();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RegularQueryContext ------------------------------------------------------------------

CypherParser::OC_RegularQueryContext::OC_RegularQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SingleQueryContext* CypherParser::OC_RegularQueryContext::oC_SingleQuery() {
  return getRuleContext<CypherParser::OC_SingleQueryContext>(0);
}

std::vector<CypherParser::OC_UnionContext *> CypherParser::OC_RegularQueryContext::oC_Union() {
  return getRuleContexts<CypherParser::OC_UnionContext>();
}

CypherParser::OC_UnionContext* CypherParser::OC_RegularQueryContext::oC_Union(size_t i) {
  return getRuleContext<CypherParser::OC_UnionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_RegularQueryContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_RegularQueryContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::OC_ReturnContext *> CypherParser::OC_RegularQueryContext::oC_Return() {
  return getRuleContexts<CypherParser::OC_ReturnContext>();
}

CypherParser::OC_ReturnContext* CypherParser::OC_RegularQueryContext::oC_Return(size_t i) {
  return getRuleContext<CypherParser::OC_ReturnContext>(i);
}


size_t CypherParser::OC_RegularQueryContext::getRuleIndex() const {
  return CypherParser::RuleOC_RegularQuery;
}


CypherParser::OC_RegularQueryContext* CypherParser::oC_RegularQuery() {
  OC_RegularQueryContext *_localctx = _tracker.createInstance<OC_RegularQueryContext>(_ctx, getState());
  enterRule(_localctx, 56, CypherParser::RuleOC_RegularQuery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(542);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 52, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(521);
      oC_SingleQuery();
      setState(528);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 49, _ctx);
      while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
        if (alt == 1) {
          setState(523);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(522);
            match(CypherParser::SP);
          }
          setState(525);
          oC_Union(); 
        }
        setState(530);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 49, _ctx);
      }
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(535); 
      _errHandler->sync(this);
      alt = 1;
      do {
        switch (alt) {
          case 1: {
                setState(531);
                oC_Return();
                setState(533);
                _errHandler->sync(this);

                switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 50, _ctx)) {
                case 1: {
                  setState(532);
                  match(CypherParser::SP);
                  break;
                }

                default:
                  break;
                }
                break;
              }

        default:
          throw NoViableAltException(this);
        }
        setState(537); 
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 51, _ctx);
      } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
      setState(539);
      oC_SingleQuery();
       notifyReturnNotAtEnd(_localctx->start); 
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_UnionContext ------------------------------------------------------------------

CypherParser::OC_UnionContext::OC_UnionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_UnionContext::UNION() {
  return getToken(CypherParser::UNION, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_UnionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_UnionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_UnionContext::ALL() {
  return getToken(CypherParser::ALL, 0);
}

CypherParser::OC_SingleQueryContext* CypherParser::OC_UnionContext::oC_SingleQuery() {
  return getRuleContext<CypherParser::OC_SingleQueryContext>(0);
}


size_t CypherParser::OC_UnionContext::getRuleIndex() const {
  return CypherParser::RuleOC_Union;
}


CypherParser::OC_UnionContext* CypherParser::oC_Union() {
  OC_UnionContext *_localctx = _tracker.createInstance<OC_UnionContext>(_ctx, getState());
  enterRule(_localctx, 58, CypherParser::RuleOC_Union);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(556);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 55, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(544);
      match(CypherParser::UNION);
      setState(545);
      match(CypherParser::SP);
      setState(546);
      match(CypherParser::ALL);
      setState(548);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 53, _ctx)) {
      case 1: {
        setState(547);
        match(CypherParser::SP);
        break;
      }

      default:
        break;
      }
      setState(550);
      oC_SingleQuery();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(551);
      match(CypherParser::UNION);
      setState(553);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 54, _ctx)) {
      case 1: {
        setState(552);
        match(CypherParser::SP);
        break;
      }

      default:
        break;
      }
      setState(555);
      oC_SingleQuery();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SingleQueryContext ------------------------------------------------------------------

CypherParser::OC_SingleQueryContext::OC_SingleQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SinglePartQueryContext* CypherParser::OC_SingleQueryContext::oC_SinglePartQuery() {
  return getRuleContext<CypherParser::OC_SinglePartQueryContext>(0);
}

CypherParser::OC_MultiPartQueryContext* CypherParser::OC_SingleQueryContext::oC_MultiPartQuery() {
  return getRuleContext<CypherParser::OC_MultiPartQueryContext>(0);
}


size_t CypherParser::OC_SingleQueryContext::getRuleIndex() const {
  return CypherParser::RuleOC_SingleQuery;
}


CypherParser::OC_SingleQueryContext* CypherParser::oC_SingleQuery() {
  OC_SingleQueryContext *_localctx = _tracker.createInstance<OC_SingleQueryContext>(_ctx, getState());
  enterRule(_localctx, 60, CypherParser::RuleOC_SingleQuery);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(560);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 56, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(558);
      oC_SinglePartQuery();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(559);
      oC_MultiPartQuery();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SinglePartQueryContext ------------------------------------------------------------------

CypherParser::OC_SinglePartQueryContext::OC_SinglePartQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ReturnContext* CypherParser::OC_SinglePartQueryContext::oC_Return() {
  return getRuleContext<CypherParser::OC_ReturnContext>(0);
}

std::vector<CypherParser::OC_ReadingClauseContext *> CypherParser::OC_SinglePartQueryContext::oC_ReadingClause() {
  return getRuleContexts<CypherParser::OC_ReadingClauseContext>();
}

CypherParser::OC_ReadingClauseContext* CypherParser::OC_SinglePartQueryContext::oC_ReadingClause(size_t i) {
  return getRuleContext<CypherParser::OC_ReadingClauseContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_SinglePartQueryContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_SinglePartQueryContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::OC_UpdatingClauseContext *> CypherParser::OC_SinglePartQueryContext::oC_UpdatingClause() {
  return getRuleContexts<CypherParser::OC_UpdatingClauseContext>();
}

CypherParser::OC_UpdatingClauseContext* CypherParser::OC_SinglePartQueryContext::oC_UpdatingClause(size_t i) {
  return getRuleContext<CypherParser::OC_UpdatingClauseContext>(i);
}


size_t CypherParser::OC_SinglePartQueryContext::getRuleIndex() const {
  return CypherParser::RuleOC_SinglePartQuery;
}


CypherParser::OC_SinglePartQueryContext* CypherParser::oC_SinglePartQuery() {
  OC_SinglePartQueryContext *_localctx = _tracker.createInstance<OC_SinglePartQueryContext>(_ctx, getState());
  enterRule(_localctx, 62, CypherParser::RuleOC_SinglePartQuery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(607);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 67, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(568);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (((((_la - 63) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 63)) & ((1ULL << (CypherParser::OPTIONAL - 63))
        | (1ULL << (CypherParser::MATCH - 63))
        | (1ULL << (CypherParser::UNWIND - 63)))) != 0)) {
        setState(562);
        oC_ReadingClause();
        setState(564);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(563);
          match(CypherParser::SP);
        }
        setState(570);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(571);
      oC_Return();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(578);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (((((_la - 63) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 63)) & ((1ULL << (CypherParser::OPTIONAL - 63))
        | (1ULL << (CypherParser::MATCH - 63))
        | (1ULL << (CypherParser::UNWIND - 63)))) != 0)) {
        setState(572);
        oC_ReadingClause();
        setState(574);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(573);
          match(CypherParser::SP);
        }
        setState(580);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
      setState(581);
      oC_UpdatingClause();
      setState(588);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 62, _ctx);
      while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
        if (alt == 1) {
          setState(583);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(582);
            match(CypherParser::SP);
          }
          setState(585);
          oC_UpdatingClause(); 
        }
        setState(590);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 62, _ctx);
      }
      setState(595);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 64, _ctx)) {
      case 1: {
        setState(592);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(591);
          match(CypherParser::SP);
        }
        setState(594);
        oC_Return();
        break;
      }

      default:
        break;
      }
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(603);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (((((_la - 63) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 63)) & ((1ULL << (CypherParser::OPTIONAL - 63))
        | (1ULL << (CypherParser::MATCH - 63))
        | (1ULL << (CypherParser::UNWIND - 63)))) != 0)) {
        setState(597);
        oC_ReadingClause();
        setState(599);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 65, _ctx)) {
        case 1: {
          setState(598);
          match(CypherParser::SP);
          break;
        }

        default:
          break;
        }
        setState(605);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
       notifyQueryNotConcludeWithReturn(_localctx->start); 
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_MultiPartQueryContext ------------------------------------------------------------------

CypherParser::OC_MultiPartQueryContext::OC_MultiPartQueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SinglePartQueryContext* CypherParser::OC_MultiPartQueryContext::oC_SinglePartQuery() {
  return getRuleContext<CypherParser::OC_SinglePartQueryContext>(0);
}

std::vector<CypherParser::KU_QueryPartContext *> CypherParser::OC_MultiPartQueryContext::kU_QueryPart() {
  return getRuleContexts<CypherParser::KU_QueryPartContext>();
}

CypherParser::KU_QueryPartContext* CypherParser::OC_MultiPartQueryContext::kU_QueryPart(size_t i) {
  return getRuleContext<CypherParser::KU_QueryPartContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_MultiPartQueryContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_MultiPartQueryContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_MultiPartQueryContext::getRuleIndex() const {
  return CypherParser::RuleOC_MultiPartQuery;
}


CypherParser::OC_MultiPartQueryContext* CypherParser::oC_MultiPartQuery() {
  OC_MultiPartQueryContext *_localctx = _tracker.createInstance<OC_MultiPartQueryContext>(_ctx, getState());
  enterRule(_localctx, 64, CypherParser::RuleOC_MultiPartQuery);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(613); 
    _errHandler->sync(this);
    alt = 1;
    do {
      switch (alt) {
        case 1: {
              setState(609);
              kU_QueryPart();
              setState(611);
              _errHandler->sync(this);

              switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 68, _ctx)) {
              case 1: {
                setState(610);
                match(CypherParser::SP);
                break;
              }

              default:
                break;
              }
              break;
            }

      default:
        throw NoViableAltException(this);
      }
      setState(615); 
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 69, _ctx);
    } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
    setState(617);
    oC_SinglePartQuery();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_QueryPartContext ------------------------------------------------------------------

CypherParser::KU_QueryPartContext::KU_QueryPartContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_WithContext* CypherParser::KU_QueryPartContext::oC_With() {
  return getRuleContext<CypherParser::OC_WithContext>(0);
}

std::vector<CypherParser::OC_ReadingClauseContext *> CypherParser::KU_QueryPartContext::oC_ReadingClause() {
  return getRuleContexts<CypherParser::OC_ReadingClauseContext>();
}

CypherParser::OC_ReadingClauseContext* CypherParser::KU_QueryPartContext::oC_ReadingClause(size_t i) {
  return getRuleContext<CypherParser::OC_ReadingClauseContext>(i);
}

std::vector<CypherParser::OC_UpdatingClauseContext *> CypherParser::KU_QueryPartContext::oC_UpdatingClause() {
  return getRuleContexts<CypherParser::OC_UpdatingClauseContext>();
}

CypherParser::OC_UpdatingClauseContext* CypherParser::KU_QueryPartContext::oC_UpdatingClause(size_t i) {
  return getRuleContext<CypherParser::OC_UpdatingClauseContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_QueryPartContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_QueryPartContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_QueryPartContext::getRuleIndex() const {
  return CypherParser::RuleKU_QueryPart;
}


CypherParser::KU_QueryPartContext* CypherParser::kU_QueryPart() {
  KU_QueryPartContext *_localctx = _tracker.createInstance<KU_QueryPartContext>(_ctx, getState());
  enterRule(_localctx, 66, CypherParser::RuleKU_QueryPart);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(625);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (((((_la - 63) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 63)) & ((1ULL << (CypherParser::OPTIONAL - 63))
      | (1ULL << (CypherParser::MATCH - 63))
      | (1ULL << (CypherParser::UNWIND - 63)))) != 0)) {
      setState(619);
      oC_ReadingClause();
      setState(621);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(620);
        match(CypherParser::SP);
      }
      setState(627);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(634);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (((((_la - 66) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 66)) & ((1ULL << (CypherParser::CREATE - 66))
      | (1ULL << (CypherParser::SET - 66))
      | (1ULL << (CypherParser::DELETE - 66)))) != 0)) {
      setState(628);
      oC_UpdatingClause();
      setState(630);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(629);
        match(CypherParser::SP);
      }
      setState(636);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
    setState(637);
    oC_With();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_UpdatingClauseContext ------------------------------------------------------------------

CypherParser::OC_UpdatingClauseContext::OC_UpdatingClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_CreateContext* CypherParser::OC_UpdatingClauseContext::oC_Create() {
  return getRuleContext<CypherParser::OC_CreateContext>(0);
}

CypherParser::OC_SetContext* CypherParser::OC_UpdatingClauseContext::oC_Set() {
  return getRuleContext<CypherParser::OC_SetContext>(0);
}

CypherParser::OC_DeleteContext* CypherParser::OC_UpdatingClauseContext::oC_Delete() {
  return getRuleContext<CypherParser::OC_DeleteContext>(0);
}


size_t CypherParser::OC_UpdatingClauseContext::getRuleIndex() const {
  return CypherParser::RuleOC_UpdatingClause;
}


CypherParser::OC_UpdatingClauseContext* CypherParser::oC_UpdatingClause() {
  OC_UpdatingClauseContext *_localctx = _tracker.createInstance<OC_UpdatingClauseContext>(_ctx, getState());
  enterRule(_localctx, 68, CypherParser::RuleOC_UpdatingClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(642);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::CREATE: {
        enterOuterAlt(_localctx, 1);
        setState(639);
        oC_Create();
        break;
      }

      case CypherParser::SET: {
        enterOuterAlt(_localctx, 2);
        setState(640);
        oC_Set();
        break;
      }

      case CypherParser::DELETE: {
        enterOuterAlt(_localctx, 3);
        setState(641);
        oC_Delete();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ReadingClauseContext ------------------------------------------------------------------

CypherParser::OC_ReadingClauseContext::OC_ReadingClauseContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_MatchContext* CypherParser::OC_ReadingClauseContext::oC_Match() {
  return getRuleContext<CypherParser::OC_MatchContext>(0);
}

CypherParser::OC_UnwindContext* CypherParser::OC_ReadingClauseContext::oC_Unwind() {
  return getRuleContext<CypherParser::OC_UnwindContext>(0);
}


size_t CypherParser::OC_ReadingClauseContext::getRuleIndex() const {
  return CypherParser::RuleOC_ReadingClause;
}


CypherParser::OC_ReadingClauseContext* CypherParser::oC_ReadingClause() {
  OC_ReadingClauseContext *_localctx = _tracker.createInstance<OC_ReadingClauseContext>(_ctx, getState());
  enterRule(_localctx, 70, CypherParser::RuleOC_ReadingClause);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(646);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::OPTIONAL:
      case CypherParser::MATCH: {
        enterOuterAlt(_localctx, 1);
        setState(644);
        oC_Match();
        break;
      }

      case CypherParser::UNWIND: {
        enterOuterAlt(_localctx, 2);
        setState(645);
        oC_Unwind();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_MatchContext ------------------------------------------------------------------

CypherParser::OC_MatchContext::OC_MatchContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_MatchContext::MATCH() {
  return getToken(CypherParser::MATCH, 0);
}

CypherParser::OC_PatternContext* CypherParser::OC_MatchContext::oC_Pattern() {
  return getRuleContext<CypherParser::OC_PatternContext>(0);
}

tree::TerminalNode* CypherParser::OC_MatchContext::OPTIONAL() {
  return getToken(CypherParser::OPTIONAL, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_MatchContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_MatchContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_WhereContext* CypherParser::OC_MatchContext::oC_Where() {
  return getRuleContext<CypherParser::OC_WhereContext>(0);
}


size_t CypherParser::OC_MatchContext::getRuleIndex() const {
  return CypherParser::RuleOC_Match;
}


CypherParser::OC_MatchContext* CypherParser::oC_Match() {
  OC_MatchContext *_localctx = _tracker.createInstance<OC_MatchContext>(_ctx, getState());
  enterRule(_localctx, 72, CypherParser::RuleOC_Match);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(650);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::OPTIONAL) {
      setState(648);
      match(CypherParser::OPTIONAL);
      setState(649);
      match(CypherParser::SP);
    }
    setState(652);
    match(CypherParser::MATCH);
    setState(654);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 77, _ctx)) {
    case 1: {
      setState(653);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(656);
    oC_Pattern();
    setState(661);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 79, _ctx)) {
    case 1: {
      setState(658);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(657);
        match(CypherParser::SP);
      }
      setState(660);
      oC_Where();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_UnwindContext ------------------------------------------------------------------

CypherParser::OC_UnwindContext::OC_UnwindContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_UnwindContext::UNWIND() {
  return getToken(CypherParser::UNWIND, 0);
}

CypherParser::OC_ExpressionContext* CypherParser::OC_UnwindContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_UnwindContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_UnwindContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_UnwindContext::AS() {
  return getToken(CypherParser::AS, 0);
}

CypherParser::OC_VariableContext* CypherParser::OC_UnwindContext::oC_Variable() {
  return getRuleContext<CypherParser::OC_VariableContext>(0);
}


size_t CypherParser::OC_UnwindContext::getRuleIndex() const {
  return CypherParser::RuleOC_Unwind;
}


CypherParser::OC_UnwindContext* CypherParser::oC_Unwind() {
  OC_UnwindContext *_localctx = _tracker.createInstance<OC_UnwindContext>(_ctx, getState());
  enterRule(_localctx, 74, CypherParser::RuleOC_Unwind);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(663);
    match(CypherParser::UNWIND);
    setState(665);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(664);
      match(CypherParser::SP);
    }
    setState(667);
    oC_Expression();
    setState(668);
    match(CypherParser::SP);
    setState(669);
    match(CypherParser::AS);
    setState(670);
    match(CypherParser::SP);
    setState(671);
    oC_Variable();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_CreateContext ------------------------------------------------------------------

CypherParser::OC_CreateContext::OC_CreateContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_CreateContext::CREATE() {
  return getToken(CypherParser::CREATE, 0);
}

CypherParser::OC_PatternContext* CypherParser::OC_CreateContext::oC_Pattern() {
  return getRuleContext<CypherParser::OC_PatternContext>(0);
}

tree::TerminalNode* CypherParser::OC_CreateContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_CreateContext::getRuleIndex() const {
  return CypherParser::RuleOC_Create;
}


CypherParser::OC_CreateContext* CypherParser::oC_Create() {
  OC_CreateContext *_localctx = _tracker.createInstance<OC_CreateContext>(_ctx, getState());
  enterRule(_localctx, 76, CypherParser::RuleOC_Create);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(673);
    match(CypherParser::CREATE);
    setState(675);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 81, _ctx)) {
    case 1: {
      setState(674);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(677);
    oC_Pattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SetContext ------------------------------------------------------------------

CypherParser::OC_SetContext::OC_SetContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_SetContext::SET() {
  return getToken(CypherParser::SET, 0);
}

std::vector<CypherParser::OC_SetItemContext *> CypherParser::OC_SetContext::oC_SetItem() {
  return getRuleContexts<CypherParser::OC_SetItemContext>();
}

CypherParser::OC_SetItemContext* CypherParser::OC_SetContext::oC_SetItem(size_t i) {
  return getRuleContext<CypherParser::OC_SetItemContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_SetContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_SetContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_SetContext::getRuleIndex() const {
  return CypherParser::RuleOC_Set;
}


CypherParser::OC_SetContext* CypherParser::oC_Set() {
  OC_SetContext *_localctx = _tracker.createInstance<OC_SetContext>(_ctx, getState());
  enterRule(_localctx, 78, CypherParser::RuleOC_Set);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(679);
    match(CypherParser::SET);
    setState(681);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(680);
      match(CypherParser::SP);
    }
    setState(683);
    oC_SetItem();
    setState(694);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 85, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(685);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(684);
          match(CypherParser::SP);
        }
        setState(687);
        match(CypherParser::T__3);
        setState(689);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(688);
          match(CypherParser::SP);
        }
        setState(691);
        oC_SetItem(); 
      }
      setState(696);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 85, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SetItemContext ------------------------------------------------------------------

CypherParser::OC_SetItemContext::OC_SetItemContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_PropertyExpressionContext* CypherParser::OC_SetItemContext::oC_PropertyExpression() {
  return getRuleContext<CypherParser::OC_PropertyExpressionContext>(0);
}

CypherParser::OC_ExpressionContext* CypherParser::OC_SetItemContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_SetItemContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_SetItemContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_SetItemContext::getRuleIndex() const {
  return CypherParser::RuleOC_SetItem;
}


CypherParser::OC_SetItemContext* CypherParser::oC_SetItem() {
  OC_SetItemContext *_localctx = _tracker.createInstance<OC_SetItemContext>(_ctx, getState());
  enterRule(_localctx, 80, CypherParser::RuleOC_SetItem);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(697);
    oC_PropertyExpression();
    setState(699);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(698);
      match(CypherParser::SP);
    }
    setState(701);
    match(CypherParser::T__4);
    setState(703);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(702);
      match(CypherParser::SP);
    }
    setState(705);
    oC_Expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_DeleteContext ------------------------------------------------------------------

CypherParser::OC_DeleteContext::OC_DeleteContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_DeleteContext::DELETE() {
  return getToken(CypherParser::DELETE, 0);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::OC_DeleteContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::OC_DeleteContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_DeleteContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_DeleteContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_DeleteContext::getRuleIndex() const {
  return CypherParser::RuleOC_Delete;
}


CypherParser::OC_DeleteContext* CypherParser::oC_Delete() {
  OC_DeleteContext *_localctx = _tracker.createInstance<OC_DeleteContext>(_ctx, getState());
  enterRule(_localctx, 82, CypherParser::RuleOC_Delete);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(707);
    match(CypherParser::DELETE);
    setState(709);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(708);
      match(CypherParser::SP);
    }
    setState(711);
    oC_Expression();
    setState(722);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 91, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(713);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(712);
          match(CypherParser::SP);
        }
        setState(715);
        match(CypherParser::T__3);
        setState(717);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(716);
          match(CypherParser::SP);
        }
        setState(719);
        oC_Expression(); 
      }
      setState(724);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 91, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_WithContext ------------------------------------------------------------------

CypherParser::OC_WithContext::OC_WithContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_WithContext::WITH() {
  return getToken(CypherParser::WITH, 0);
}

CypherParser::OC_ProjectionBodyContext* CypherParser::OC_WithContext::oC_ProjectionBody() {
  return getRuleContext<CypherParser::OC_ProjectionBodyContext>(0);
}

CypherParser::OC_WhereContext* CypherParser::OC_WithContext::oC_Where() {
  return getRuleContext<CypherParser::OC_WhereContext>(0);
}

tree::TerminalNode* CypherParser::OC_WithContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_WithContext::getRuleIndex() const {
  return CypherParser::RuleOC_With;
}


CypherParser::OC_WithContext* CypherParser::oC_With() {
  OC_WithContext *_localctx = _tracker.createInstance<OC_WithContext>(_ctx, getState());
  enterRule(_localctx, 84, CypherParser::RuleOC_With);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(725);
    match(CypherParser::WITH);
    setState(726);
    oC_ProjectionBody();
    setState(731);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 93, _ctx)) {
    case 1: {
      setState(728);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(727);
        match(CypherParser::SP);
      }
      setState(730);
      oC_Where();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ReturnContext ------------------------------------------------------------------

CypherParser::OC_ReturnContext::OC_ReturnContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_ReturnContext::RETURN() {
  return getToken(CypherParser::RETURN, 0);
}

CypherParser::OC_ProjectionBodyContext* CypherParser::OC_ReturnContext::oC_ProjectionBody() {
  return getRuleContext<CypherParser::OC_ProjectionBodyContext>(0);
}


size_t CypherParser::OC_ReturnContext::getRuleIndex() const {
  return CypherParser::RuleOC_Return;
}


CypherParser::OC_ReturnContext* CypherParser::oC_Return() {
  OC_ReturnContext *_localctx = _tracker.createInstance<OC_ReturnContext>(_ctx, getState());
  enterRule(_localctx, 86, CypherParser::RuleOC_Return);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(733);
    match(CypherParser::RETURN);
    setState(734);
    oC_ProjectionBody();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ProjectionBodyContext ------------------------------------------------------------------

CypherParser::OC_ProjectionBodyContext::OC_ProjectionBodyContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::OC_ProjectionBodyContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ProjectionBodyContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_ProjectionItemsContext* CypherParser::OC_ProjectionBodyContext::oC_ProjectionItems() {
  return getRuleContext<CypherParser::OC_ProjectionItemsContext>(0);
}

tree::TerminalNode* CypherParser::OC_ProjectionBodyContext::DISTINCT() {
  return getToken(CypherParser::DISTINCT, 0);
}

CypherParser::OC_OrderContext* CypherParser::OC_ProjectionBodyContext::oC_Order() {
  return getRuleContext<CypherParser::OC_OrderContext>(0);
}

CypherParser::OC_SkipContext* CypherParser::OC_ProjectionBodyContext::oC_Skip() {
  return getRuleContext<CypherParser::OC_SkipContext>(0);
}

CypherParser::OC_LimitContext* CypherParser::OC_ProjectionBodyContext::oC_Limit() {
  return getRuleContext<CypherParser::OC_LimitContext>(0);
}


size_t CypherParser::OC_ProjectionBodyContext::getRuleIndex() const {
  return CypherParser::RuleOC_ProjectionBody;
}


CypherParser::OC_ProjectionBodyContext* CypherParser::oC_ProjectionBody() {
  OC_ProjectionBodyContext *_localctx = _tracker.createInstance<OC_ProjectionBodyContext>(_ctx, getState());
  enterRule(_localctx, 88, CypherParser::RuleOC_ProjectionBody);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(740);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 95, _ctx)) {
    case 1: {
      setState(737);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(736);
        match(CypherParser::SP);
      }
      setState(739);
      match(CypherParser::DISTINCT);
      break;
    }

    default:
      break;
    }
    setState(742);
    match(CypherParser::SP);
    setState(743);
    oC_ProjectionItems();
    setState(746);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 96, _ctx)) {
    case 1: {
      setState(744);
      match(CypherParser::SP);
      setState(745);
      oC_Order();
      break;
    }

    default:
      break;
    }
    setState(750);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 97, _ctx)) {
    case 1: {
      setState(748);
      match(CypherParser::SP);
      setState(749);
      oC_Skip();
      break;
    }

    default:
      break;
    }
    setState(754);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 98, _ctx)) {
    case 1: {
      setState(752);
      match(CypherParser::SP);
      setState(753);
      oC_Limit();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ProjectionItemsContext ------------------------------------------------------------------

CypherParser::OC_ProjectionItemsContext::OC_ProjectionItemsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_ProjectionItemsContext::STAR() {
  return getToken(CypherParser::STAR, 0);
}

std::vector<CypherParser::OC_ProjectionItemContext *> CypherParser::OC_ProjectionItemsContext::oC_ProjectionItem() {
  return getRuleContexts<CypherParser::OC_ProjectionItemContext>();
}

CypherParser::OC_ProjectionItemContext* CypherParser::OC_ProjectionItemsContext::oC_ProjectionItem(size_t i) {
  return getRuleContext<CypherParser::OC_ProjectionItemContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_ProjectionItemsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ProjectionItemsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_ProjectionItemsContext::getRuleIndex() const {
  return CypherParser::RuleOC_ProjectionItems;
}


CypherParser::OC_ProjectionItemsContext* CypherParser::oC_ProjectionItems() {
  OC_ProjectionItemsContext *_localctx = _tracker.createInstance<OC_ProjectionItemsContext>(_ctx, getState());
  enterRule(_localctx, 90, CypherParser::RuleOC_ProjectionItems);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(784);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::STAR: {
        enterOuterAlt(_localctx, 1);
        setState(756);
        match(CypherParser::STAR);
        setState(767);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 101, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
          if (alt == 1) {
            setState(758);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(757);
              match(CypherParser::SP);
            }
            setState(760);
            match(CypherParser::T__3);
            setState(762);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(761);
              match(CypherParser::SP);
            }
            setState(764);
            oC_ProjectionItem(); 
          }
          setState(769);
          _errHandler->sync(this);
          alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 101, _ctx);
        }
        break;
      }

      case CypherParser::T__1:
      case CypherParser::T__6:
      case CypherParser::T__25:
      case CypherParser::NOT:
      case CypherParser::MINUS:
      case CypherParser::NULL_:
      case CypherParser::TRUE:
      case CypherParser::FALSE:
      case CypherParser::EXISTS:
      case CypherParser::CASE:
      case CypherParser::StringLiteral:
      case CypherParser::DecimalInteger:
      case CypherParser::HexLetter:
      case CypherParser::RegularDecimalReal:
      case CypherParser::UnescapedSymbolicName:
      case CypherParser::EscapedSymbolicName: {
        enterOuterAlt(_localctx, 2);
        setState(770);
        oC_ProjectionItem();
        setState(781);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 104, _ctx);
        while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
          if (alt == 1) {
            setState(772);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(771);
              match(CypherParser::SP);
            }
            setState(774);
            match(CypherParser::T__3);
            setState(776);
            _errHandler->sync(this);

            _la = _input->LA(1);
            if (_la == CypherParser::SP) {
              setState(775);
              match(CypherParser::SP);
            }
            setState(778);
            oC_ProjectionItem(); 
          }
          setState(783);
          _errHandler->sync(this);
          alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 104, _ctx);
        }
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ProjectionItemContext ------------------------------------------------------------------

CypherParser::OC_ProjectionItemContext::OC_ProjectionItemContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ExpressionContext* CypherParser::OC_ProjectionItemContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_ProjectionItemContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ProjectionItemContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_ProjectionItemContext::AS() {
  return getToken(CypherParser::AS, 0);
}

CypherParser::OC_VariableContext* CypherParser::OC_ProjectionItemContext::oC_Variable() {
  return getRuleContext<CypherParser::OC_VariableContext>(0);
}


size_t CypherParser::OC_ProjectionItemContext::getRuleIndex() const {
  return CypherParser::RuleOC_ProjectionItem;
}


CypherParser::OC_ProjectionItemContext* CypherParser::oC_ProjectionItem() {
  OC_ProjectionItemContext *_localctx = _tracker.createInstance<OC_ProjectionItemContext>(_ctx, getState());
  enterRule(_localctx, 92, CypherParser::RuleOC_ProjectionItem);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(793);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 106, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(786);
      oC_Expression();
      setState(787);
      match(CypherParser::SP);
      setState(788);
      match(CypherParser::AS);
      setState(789);
      match(CypherParser::SP);
      setState(790);
      oC_Variable();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(792);
      oC_Expression();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_OrderContext ------------------------------------------------------------------

CypherParser::OC_OrderContext::OC_OrderContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_OrderContext::ORDER() {
  return getToken(CypherParser::ORDER, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_OrderContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_OrderContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_OrderContext::BY() {
  return getToken(CypherParser::BY, 0);
}

std::vector<CypherParser::OC_SortItemContext *> CypherParser::OC_OrderContext::oC_SortItem() {
  return getRuleContexts<CypherParser::OC_SortItemContext>();
}

CypherParser::OC_SortItemContext* CypherParser::OC_OrderContext::oC_SortItem(size_t i) {
  return getRuleContext<CypherParser::OC_SortItemContext>(i);
}


size_t CypherParser::OC_OrderContext::getRuleIndex() const {
  return CypherParser::RuleOC_Order;
}


CypherParser::OC_OrderContext* CypherParser::oC_Order() {
  OC_OrderContext *_localctx = _tracker.createInstance<OC_OrderContext>(_ctx, getState());
  enterRule(_localctx, 94, CypherParser::RuleOC_Order);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(795);
    match(CypherParser::ORDER);
    setState(796);
    match(CypherParser::SP);
    setState(797);
    match(CypherParser::BY);
    setState(798);
    match(CypherParser::SP);
    setState(799);
    oC_SortItem();
    setState(807);
    _errHandler->sync(this);
    _la = _input->LA(1);
    while (_la == CypherParser::T__3) {
      setState(800);
      match(CypherParser::T__3);
      setState(802);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(801);
        match(CypherParser::SP);
      }
      setState(804);
      oC_SortItem();
      setState(809);
      _errHandler->sync(this);
      _la = _input->LA(1);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SkipContext ------------------------------------------------------------------

CypherParser::OC_SkipContext::OC_SkipContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_SkipContext::L_SKIP() {
  return getToken(CypherParser::L_SKIP, 0);
}

tree::TerminalNode* CypherParser::OC_SkipContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::OC_ExpressionContext* CypherParser::OC_SkipContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}


size_t CypherParser::OC_SkipContext::getRuleIndex() const {
  return CypherParser::RuleOC_Skip;
}


CypherParser::OC_SkipContext* CypherParser::oC_Skip() {
  OC_SkipContext *_localctx = _tracker.createInstance<OC_SkipContext>(_ctx, getState());
  enterRule(_localctx, 96, CypherParser::RuleOC_Skip);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(810);
    match(CypherParser::L_SKIP);
    setState(811);
    match(CypherParser::SP);
    setState(812);
    oC_Expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_LimitContext ------------------------------------------------------------------

CypherParser::OC_LimitContext::OC_LimitContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_LimitContext::LIMIT() {
  return getToken(CypherParser::LIMIT, 0);
}

tree::TerminalNode* CypherParser::OC_LimitContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::OC_ExpressionContext* CypherParser::OC_LimitContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}


size_t CypherParser::OC_LimitContext::getRuleIndex() const {
  return CypherParser::RuleOC_Limit;
}


CypherParser::OC_LimitContext* CypherParser::oC_Limit() {
  OC_LimitContext *_localctx = _tracker.createInstance<OC_LimitContext>(_ctx, getState());
  enterRule(_localctx, 98, CypherParser::RuleOC_Limit);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(814);
    match(CypherParser::LIMIT);
    setState(815);
    match(CypherParser::SP);
    setState(816);
    oC_Expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SortItemContext ------------------------------------------------------------------

CypherParser::OC_SortItemContext::OC_SortItemContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ExpressionContext* CypherParser::OC_SortItemContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

tree::TerminalNode* CypherParser::OC_SortItemContext::ASCENDING() {
  return getToken(CypherParser::ASCENDING, 0);
}

tree::TerminalNode* CypherParser::OC_SortItemContext::ASC() {
  return getToken(CypherParser::ASC, 0);
}

tree::TerminalNode* CypherParser::OC_SortItemContext::DESCENDING() {
  return getToken(CypherParser::DESCENDING, 0);
}

tree::TerminalNode* CypherParser::OC_SortItemContext::DESC() {
  return getToken(CypherParser::DESC, 0);
}

tree::TerminalNode* CypherParser::OC_SortItemContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_SortItemContext::getRuleIndex() const {
  return CypherParser::RuleOC_SortItem;
}


CypherParser::OC_SortItemContext* CypherParser::oC_SortItem() {
  OC_SortItemContext *_localctx = _tracker.createInstance<OC_SortItemContext>(_ctx, getState());
  enterRule(_localctx, 100, CypherParser::RuleOC_SortItem);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(818);
    oC_Expression();
    setState(823);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 110, _ctx)) {
    case 1: {
      setState(820);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(819);
        match(CypherParser::SP);
      }
      setState(822);
      _la = _input->LA(1);
      if (!(((((_la - 78) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 78)) & ((1ULL << (CypherParser::ASCENDING - 78))
        | (1ULL << (CypherParser::ASC - 78))
        | (1ULL << (CypherParser::DESCENDING - 78))
        | (1ULL << (CypherParser::DESC - 78)))) != 0))) {
      _errHandler->recoverInline(this);
      }
      else {
        _errHandler->reportMatch(this);
        consume();
      }
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_WhereContext ------------------------------------------------------------------

CypherParser::OC_WhereContext::OC_WhereContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_WhereContext::WHERE() {
  return getToken(CypherParser::WHERE, 0);
}

tree::TerminalNode* CypherParser::OC_WhereContext::SP() {
  return getToken(CypherParser::SP, 0);
}

CypherParser::OC_ExpressionContext* CypherParser::OC_WhereContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}


size_t CypherParser::OC_WhereContext::getRuleIndex() const {
  return CypherParser::RuleOC_Where;
}


CypherParser::OC_WhereContext* CypherParser::oC_Where() {
  OC_WhereContext *_localctx = _tracker.createInstance<OC_WhereContext>(_ctx, getState());
  enterRule(_localctx, 102, CypherParser::RuleOC_Where);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(825);
    match(CypherParser::WHERE);
    setState(826);
    match(CypherParser::SP);
    setState(827);
    oC_Expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PatternContext ------------------------------------------------------------------

CypherParser::OC_PatternContext::OC_PatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_PatternPartContext *> CypherParser::OC_PatternContext::oC_PatternPart() {
  return getRuleContexts<CypherParser::OC_PatternPartContext>();
}

CypherParser::OC_PatternPartContext* CypherParser::OC_PatternContext::oC_PatternPart(size_t i) {
  return getRuleContext<CypherParser::OC_PatternPartContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_PatternContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_PatternContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_PatternContext::getRuleIndex() const {
  return CypherParser::RuleOC_Pattern;
}


CypherParser::OC_PatternContext* CypherParser::oC_Pattern() {
  OC_PatternContext *_localctx = _tracker.createInstance<OC_PatternContext>(_ctx, getState());
  enterRule(_localctx, 104, CypherParser::RuleOC_Pattern);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(829);
    oC_PatternPart();
    setState(840);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 113, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(831);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(830);
          match(CypherParser::SP);
        }
        setState(833);
        match(CypherParser::T__3);
        setState(835);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 112, _ctx)) {
        case 1: {
          setState(834);
          match(CypherParser::SP);
          break;
        }

        default:
          break;
        }
        setState(837);
        oC_PatternPart(); 
      }
      setState(842);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 113, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PatternPartContext ------------------------------------------------------------------

CypherParser::OC_PatternPartContext::OC_PatternPartContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_AnonymousPatternPartContext* CypherParser::OC_PatternPartContext::oC_AnonymousPatternPart() {
  return getRuleContext<CypherParser::OC_AnonymousPatternPartContext>(0);
}


size_t CypherParser::OC_PatternPartContext::getRuleIndex() const {
  return CypherParser::RuleOC_PatternPart;
}


CypherParser::OC_PatternPartContext* CypherParser::oC_PatternPart() {
  OC_PatternPartContext *_localctx = _tracker.createInstance<OC_PatternPartContext>(_ctx, getState());
  enterRule(_localctx, 106, CypherParser::RuleOC_PatternPart);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(843);
    oC_AnonymousPatternPart();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_AnonymousPatternPartContext ------------------------------------------------------------------

CypherParser::OC_AnonymousPatternPartContext::OC_AnonymousPatternPartContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_PatternElementContext* CypherParser::OC_AnonymousPatternPartContext::oC_PatternElement() {
  return getRuleContext<CypherParser::OC_PatternElementContext>(0);
}


size_t CypherParser::OC_AnonymousPatternPartContext::getRuleIndex() const {
  return CypherParser::RuleOC_AnonymousPatternPart;
}


CypherParser::OC_AnonymousPatternPartContext* CypherParser::oC_AnonymousPatternPart() {
  OC_AnonymousPatternPartContext *_localctx = _tracker.createInstance<OC_AnonymousPatternPartContext>(_ctx, getState());
  enterRule(_localctx, 108, CypherParser::RuleOC_AnonymousPatternPart);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(845);
    oC_PatternElement();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PatternElementContext ------------------------------------------------------------------

CypherParser::OC_PatternElementContext::OC_PatternElementContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_NodePatternContext* CypherParser::OC_PatternElementContext::oC_NodePattern() {
  return getRuleContext<CypherParser::OC_NodePatternContext>(0);
}

std::vector<CypherParser::OC_PatternElementChainContext *> CypherParser::OC_PatternElementContext::oC_PatternElementChain() {
  return getRuleContexts<CypherParser::OC_PatternElementChainContext>();
}

CypherParser::OC_PatternElementChainContext* CypherParser::OC_PatternElementContext::oC_PatternElementChain(size_t i) {
  return getRuleContext<CypherParser::OC_PatternElementChainContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_PatternElementContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_PatternElementContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_PatternElementContext* CypherParser::OC_PatternElementContext::oC_PatternElement() {
  return getRuleContext<CypherParser::OC_PatternElementContext>(0);
}


size_t CypherParser::OC_PatternElementContext::getRuleIndex() const {
  return CypherParser::RuleOC_PatternElement;
}


CypherParser::OC_PatternElementContext* CypherParser::oC_PatternElement() {
  OC_PatternElementContext *_localctx = _tracker.createInstance<OC_PatternElementContext>(_ctx, getState());
  enterRule(_localctx, 110, CypherParser::RuleOC_PatternElement);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(861);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 116, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(847);
      oC_NodePattern();
      setState(854);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 115, _ctx);
      while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
        if (alt == 1) {
          setState(849);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(848);
            match(CypherParser::SP);
          }
          setState(851);
          oC_PatternElementChain(); 
        }
        setState(856);
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 115, _ctx);
      }
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(857);
      match(CypherParser::T__1);
      setState(858);
      oC_PatternElement();
      setState(859);
      match(CypherParser::T__2);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_NodePatternContext ------------------------------------------------------------------

CypherParser::OC_NodePatternContext::OC_NodePatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::OC_NodePatternContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_NodePatternContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_VariableContext* CypherParser::OC_NodePatternContext::oC_Variable() {
  return getRuleContext<CypherParser::OC_VariableContext>(0);
}

CypherParser::OC_NodeLabelsContext* CypherParser::OC_NodePatternContext::oC_NodeLabels() {
  return getRuleContext<CypherParser::OC_NodeLabelsContext>(0);
}

CypherParser::KU_PropertiesContext* CypherParser::OC_NodePatternContext::kU_Properties() {
  return getRuleContext<CypherParser::KU_PropertiesContext>(0);
}


size_t CypherParser::OC_NodePatternContext::getRuleIndex() const {
  return CypherParser::RuleOC_NodePattern;
}


CypherParser::OC_NodePatternContext* CypherParser::oC_NodePattern() {
  OC_NodePatternContext *_localctx = _tracker.createInstance<OC_NodePatternContext>(_ctx, getState());
  enterRule(_localctx, 112, CypherParser::RuleOC_NodePattern);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(908);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::T__1: {
        enterOuterAlt(_localctx, 1);
        setState(863);
        match(CypherParser::T__1);
        setState(865);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(864);
          match(CypherParser::SP);
        }
        setState(871);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (((((_la - 106) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 106)) & ((1ULL << (CypherParser::HexLetter - 106))
          | (1ULL << (CypherParser::UnescapedSymbolicName - 106))
          | (1ULL << (CypherParser::EscapedSymbolicName - 106)))) != 0)) {
          setState(867);
          oC_Variable();
          setState(869);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(868);
            match(CypherParser::SP);
          }
        }
        setState(877);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__9) {
          setState(873);
          oC_NodeLabels();
          setState(875);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(874);
            match(CypherParser::SP);
          }
        }
        setState(883);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__8) {
          setState(879);
          kU_Properties();
          setState(881);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(880);
            match(CypherParser::SP);
          }
        }
        setState(885);
        match(CypherParser::T__2);
        break;
      }

      case CypherParser::EOF:
      case CypherParser::T__0:
      case CypherParser::T__2:
      case CypherParser::T__3:
      case CypherParser::T__8:
      case CypherParser::T__9:
      case CypherParser::T__10:
      case CypherParser::T__13:
      case CypherParser::T__26:
      case CypherParser::T__27:
      case CypherParser::T__28:
      case CypherParser::T__29:
      case CypherParser::T__34:
      case CypherParser::T__35:
      case CypherParser::T__36:
      case CypherParser::T__37:
      case CypherParser::T__38:
      case CypherParser::T__39:
      case CypherParser::T__40:
      case CypherParser::T__41:
      case CypherParser::T__42:
      case CypherParser::T__43:
      case CypherParser::T__44:
      case CypherParser::UNION:
      case CypherParser::OPTIONAL:
      case CypherParser::MATCH:
      case CypherParser::UNWIND:
      case CypherParser::CREATE:
      case CypherParser::SET:
      case CypherParser::DELETE:
      case CypherParser::WITH:
      case CypherParser::RETURN:
      case CypherParser::WHERE:
      case CypherParser::MINUS:
      case CypherParser::HexLetter:
      case CypherParser::UnescapedSymbolicName:
      case CypherParser::EscapedSymbolicName:
      case CypherParser::SP: {
        enterOuterAlt(_localctx, 2);
        setState(887);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 124, _ctx)) {
        case 1: {
          setState(886);
          match(CypherParser::SP);
          break;
        }

        default:
          break;
        }
        setState(893);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (((((_la - 106) & ~ 0x3fULL) == 0) &&
          ((1ULL << (_la - 106)) & ((1ULL << (CypherParser::HexLetter - 106))
          | (1ULL << (CypherParser::UnescapedSymbolicName - 106))
          | (1ULL << (CypherParser::EscapedSymbolicName - 106)))) != 0)) {
          setState(889);
          dynamic_cast<OC_NodePatternContext *>(_localctx)->oC_VariableContext = oC_Variable();
          setState(891);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 125, _ctx)) {
          case 1: {
            setState(890);
            match(CypherParser::SP);
            break;
          }

          default:
            break;
          }
        }
        setState(899);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__9) {
          setState(895);
          oC_NodeLabels();
          setState(897);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 127, _ctx)) {
          case 1: {
            setState(896);
            match(CypherParser::SP);
            break;
          }

          default:
            break;
          }
        }
        setState(905);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__8) {
          setState(901);
          kU_Properties();
          setState(903);
          _errHandler->sync(this);

          switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 129, _ctx)) {
          case 1: {
            setState(902);
            match(CypherParser::SP);
            break;
          }

          default:
            break;
          }
        }
         notifyNodePatternWithoutParentheses((dynamic_cast<OC_NodePatternContext *>(_localctx)->oC_VariableContext != nullptr ? _input->getText(dynamic_cast<OC_NodePatternContext *>(_localctx)->oC_VariableContext->start, dynamic_cast<OC_NodePatternContext *>(_localctx)->oC_VariableContext->stop) : nullptr), (dynamic_cast<OC_NodePatternContext *>(_localctx)->oC_VariableContext != nullptr ? (dynamic_cast<OC_NodePatternContext *>(_localctx)->oC_VariableContext->start) : nullptr)); 
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PatternElementChainContext ------------------------------------------------------------------

CypherParser::OC_PatternElementChainContext::OC_PatternElementChainContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_RelationshipPatternContext* CypherParser::OC_PatternElementChainContext::oC_RelationshipPattern() {
  return getRuleContext<CypherParser::OC_RelationshipPatternContext>(0);
}

CypherParser::OC_NodePatternContext* CypherParser::OC_PatternElementChainContext::oC_NodePattern() {
  return getRuleContext<CypherParser::OC_NodePatternContext>(0);
}

tree::TerminalNode* CypherParser::OC_PatternElementChainContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_PatternElementChainContext::getRuleIndex() const {
  return CypherParser::RuleOC_PatternElementChain;
}


CypherParser::OC_PatternElementChainContext* CypherParser::oC_PatternElementChain() {
  OC_PatternElementChainContext *_localctx = _tracker.createInstance<OC_PatternElementChainContext>(_ctx, getState());
  enterRule(_localctx, 114, CypherParser::RuleOC_PatternElementChain);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(910);
    oC_RelationshipPattern();
    setState(912);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 132, _ctx)) {
    case 1: {
      setState(911);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(914);
    oC_NodePattern();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RelationshipPatternContext ------------------------------------------------------------------

CypherParser::OC_RelationshipPatternContext::OC_RelationshipPatternContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_LeftArrowHeadContext* CypherParser::OC_RelationshipPatternContext::oC_LeftArrowHead() {
  return getRuleContext<CypherParser::OC_LeftArrowHeadContext>(0);
}

std::vector<CypherParser::OC_DashContext *> CypherParser::OC_RelationshipPatternContext::oC_Dash() {
  return getRuleContexts<CypherParser::OC_DashContext>();
}

CypherParser::OC_DashContext* CypherParser::OC_RelationshipPatternContext::oC_Dash(size_t i) {
  return getRuleContext<CypherParser::OC_DashContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_RelationshipPatternContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_RelationshipPatternContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_RelationshipDetailContext* CypherParser::OC_RelationshipPatternContext::oC_RelationshipDetail() {
  return getRuleContext<CypherParser::OC_RelationshipDetailContext>(0);
}

CypherParser::OC_RightArrowHeadContext* CypherParser::OC_RelationshipPatternContext::oC_RightArrowHead() {
  return getRuleContext<CypherParser::OC_RightArrowHeadContext>(0);
}


size_t CypherParser::OC_RelationshipPatternContext::getRuleIndex() const {
  return CypherParser::RuleOC_RelationshipPattern;
}


CypherParser::OC_RelationshipPatternContext* CypherParser::oC_RelationshipPattern() {
  OC_RelationshipPatternContext *_localctx = _tracker.createInstance<OC_RelationshipPatternContext>(_ctx, getState());
  enterRule(_localctx, 116, CypherParser::RuleOC_RelationshipPattern);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(948);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::T__13:
      case CypherParser::T__26:
      case CypherParser::T__27:
      case CypherParser::T__28:
      case CypherParser::T__29: {
        enterOuterAlt(_localctx, 1);
        setState(916);
        oC_LeftArrowHead();
        setState(918);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(917);
          match(CypherParser::SP);
        }
        setState(920);
        oC_Dash();
        setState(922);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 134, _ctx)) {
        case 1: {
          setState(921);
          match(CypherParser::SP);
          break;
        }

        default:
          break;
        }
        setState(925);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__6) {
          setState(924);
          oC_RelationshipDetail();
        }
        setState(928);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(927);
          match(CypherParser::SP);
        }
        setState(930);
        oC_Dash();
        break;
      }

      case CypherParser::T__34:
      case CypherParser::T__35:
      case CypherParser::T__36:
      case CypherParser::T__37:
      case CypherParser::T__38:
      case CypherParser::T__39:
      case CypherParser::T__40:
      case CypherParser::T__41:
      case CypherParser::T__42:
      case CypherParser::T__43:
      case CypherParser::T__44:
      case CypherParser::MINUS: {
        enterOuterAlt(_localctx, 2);
        setState(932);
        oC_Dash();
        setState(934);
        _errHandler->sync(this);

        switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 137, _ctx)) {
        case 1: {
          setState(933);
          match(CypherParser::SP);
          break;
        }

        default:
          break;
        }
        setState(937);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__6) {
          setState(936);
          oC_RelationshipDetail();
        }
        setState(940);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(939);
          match(CypherParser::SP);
        }
        setState(942);
        oC_Dash();
        setState(944);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(943);
          match(CypherParser::SP);
        }
        setState(946);
        oC_RightArrowHead();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RelationshipDetailContext ------------------------------------------------------------------

CypherParser::OC_RelationshipDetailContext::OC_RelationshipDetailContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::OC_RelationshipDetailContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_RelationshipDetailContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_VariableContext* CypherParser::OC_RelationshipDetailContext::oC_Variable() {
  return getRuleContext<CypherParser::OC_VariableContext>(0);
}

CypherParser::OC_RelationshipTypesContext* CypherParser::OC_RelationshipDetailContext::oC_RelationshipTypes() {
  return getRuleContext<CypherParser::OC_RelationshipTypesContext>(0);
}

CypherParser::OC_RangeLiteralContext* CypherParser::OC_RelationshipDetailContext::oC_RangeLiteral() {
  return getRuleContext<CypherParser::OC_RangeLiteralContext>(0);
}

CypherParser::KU_PropertiesContext* CypherParser::OC_RelationshipDetailContext::kU_Properties() {
  return getRuleContext<CypherParser::KU_PropertiesContext>(0);
}


size_t CypherParser::OC_RelationshipDetailContext::getRuleIndex() const {
  return CypherParser::RuleOC_RelationshipDetail;
}


CypherParser::OC_RelationshipDetailContext* CypherParser::oC_RelationshipDetail() {
  OC_RelationshipDetailContext *_localctx = _tracker.createInstance<OC_RelationshipDetailContext>(_ctx, getState());
  enterRule(_localctx, 118, CypherParser::RuleOC_RelationshipDetail);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(950);
    match(CypherParser::T__6);
    setState(952);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(951);
      match(CypherParser::SP);
    }
    setState(958);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (((((_la - 106) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 106)) & ((1ULL << (CypherParser::HexLetter - 106))
      | (1ULL << (CypherParser::UnescapedSymbolicName - 106))
      | (1ULL << (CypherParser::EscapedSymbolicName - 106)))) != 0)) {
      setState(954);
      oC_Variable();
      setState(956);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(955);
        match(CypherParser::SP);
      }
    }
    setState(964);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__9) {
      setState(960);
      oC_RelationshipTypes();
      setState(962);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(961);
        match(CypherParser::SP);
      }
    }
    setState(970);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::STAR) {
      setState(966);
      oC_RangeLiteral();
      setState(968);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(967);
        match(CypherParser::SP);
      }
    }
    setState(976);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::T__8) {
      setState(972);
      kU_Properties();
      setState(974);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(973);
        match(CypherParser::SP);
      }
    }
    setState(978);
    match(CypherParser::T__7);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_PropertiesContext ------------------------------------------------------------------

CypherParser::KU_PropertiesContext::KU_PropertiesContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::KU_PropertiesContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_PropertiesContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::OC_PropertyKeyNameContext *> CypherParser::KU_PropertiesContext::oC_PropertyKeyName() {
  return getRuleContexts<CypherParser::OC_PropertyKeyNameContext>();
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::KU_PropertiesContext::oC_PropertyKeyName(size_t i) {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(i);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::KU_PropertiesContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::KU_PropertiesContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}


size_t CypherParser::KU_PropertiesContext::getRuleIndex() const {
  return CypherParser::RuleKU_Properties;
}


CypherParser::KU_PropertiesContext* CypherParser::kU_Properties() {
  KU_PropertiesContext *_localctx = _tracker.createInstance<KU_PropertiesContext>(_ctx, getState());
  enterRule(_localctx, 120, CypherParser::RuleKU_Properties);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(980);
    match(CypherParser::T__8);
    setState(982);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(981);
      match(CypherParser::SP);
    }
    setState(1017);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (((((_la - 106) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 106)) & ((1ULL << (CypherParser::HexLetter - 106))
      | (1ULL << (CypherParser::UnescapedSymbolicName - 106))
      | (1ULL << (CypherParser::EscapedSymbolicName - 106)))) != 0)) {
      setState(984);
      oC_PropertyKeyName();
      setState(986);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(985);
        match(CypherParser::SP);
      }
      setState(988);
      match(CypherParser::T__9);
      setState(990);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(989);
        match(CypherParser::SP);
      }
      setState(992);
      oC_Expression();
      setState(994);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(993);
        match(CypherParser::SP);
      }
      setState(1014);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == CypherParser::T__3) {
        setState(996);
        match(CypherParser::T__3);
        setState(998);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(997);
          match(CypherParser::SP);
        }
        setState(1000);
        oC_PropertyKeyName();
        setState(1002);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1001);
          match(CypherParser::SP);
        }
        setState(1004);
        match(CypherParser::T__9);
        setState(1006);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1005);
          match(CypherParser::SP);
        }
        setState(1008);
        oC_Expression();
        setState(1010);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1009);
          match(CypherParser::SP);
        }
        setState(1016);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1019);
    match(CypherParser::T__10);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RelationshipTypesContext ------------------------------------------------------------------

CypherParser::OC_RelationshipTypesContext::OC_RelationshipTypesContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_RelTypeNameContext *> CypherParser::OC_RelationshipTypesContext::oC_RelTypeName() {
  return getRuleContexts<CypherParser::OC_RelTypeNameContext>();
}

CypherParser::OC_RelTypeNameContext* CypherParser::OC_RelationshipTypesContext::oC_RelTypeName(size_t i) {
  return getRuleContext<CypherParser::OC_RelTypeNameContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_RelationshipTypesContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_RelationshipTypesContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_RelationshipTypesContext::getRuleIndex() const {
  return CypherParser::RuleOC_RelationshipTypes;
}


CypherParser::OC_RelationshipTypesContext* CypherParser::oC_RelationshipTypes() {
  OC_RelationshipTypesContext *_localctx = _tracker.createInstance<OC_RelationshipTypesContext>(_ctx, getState());
  enterRule(_localctx, 122, CypherParser::RuleOC_RelationshipTypes);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1021);
    match(CypherParser::T__9);
    setState(1023);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1022);
      match(CypherParser::SP);
    }
    setState(1025);
    oC_RelTypeName();
    setState(1039);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 165, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1027);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1026);
          match(CypherParser::SP);
        }
        setState(1029);
        match(CypherParser::T__5);
        setState(1031);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::T__9) {
          setState(1030);
          match(CypherParser::T__9);
        }
        setState(1034);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1033);
          match(CypherParser::SP);
        }
        setState(1036);
        oC_RelTypeName(); 
      }
      setState(1041);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 165, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_NodeLabelsContext ------------------------------------------------------------------

CypherParser::OC_NodeLabelsContext::OC_NodeLabelsContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_NodeLabelContext *> CypherParser::OC_NodeLabelsContext::oC_NodeLabel() {
  return getRuleContexts<CypherParser::OC_NodeLabelContext>();
}

CypherParser::OC_NodeLabelContext* CypherParser::OC_NodeLabelsContext::oC_NodeLabel(size_t i) {
  return getRuleContext<CypherParser::OC_NodeLabelContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_NodeLabelsContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_NodeLabelsContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_NodeLabelsContext::getRuleIndex() const {
  return CypherParser::RuleOC_NodeLabels;
}


CypherParser::OC_NodeLabelsContext* CypherParser::oC_NodeLabels() {
  OC_NodeLabelsContext *_localctx = _tracker.createInstance<OC_NodeLabelsContext>(_ctx, getState());
  enterRule(_localctx, 124, CypherParser::RuleOC_NodeLabels);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1042);
    oC_NodeLabel();
    setState(1049);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 167, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1044);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1043);
          match(CypherParser::SP);
        }
        setState(1046);
        oC_NodeLabel(); 
      }
      setState(1051);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 167, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_NodeLabelContext ------------------------------------------------------------------

CypherParser::OC_NodeLabelContext::OC_NodeLabelContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_LabelNameContext* CypherParser::OC_NodeLabelContext::oC_LabelName() {
  return getRuleContext<CypherParser::OC_LabelNameContext>(0);
}

tree::TerminalNode* CypherParser::OC_NodeLabelContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_NodeLabelContext::getRuleIndex() const {
  return CypherParser::RuleOC_NodeLabel;
}


CypherParser::OC_NodeLabelContext* CypherParser::oC_NodeLabel() {
  OC_NodeLabelContext *_localctx = _tracker.createInstance<OC_NodeLabelContext>(_ctx, getState());
  enterRule(_localctx, 126, CypherParser::RuleOC_NodeLabel);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1052);
    match(CypherParser::T__9);
    setState(1054);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1053);
      match(CypherParser::SP);
    }
    setState(1056);
    oC_LabelName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RangeLiteralContext ------------------------------------------------------------------

CypherParser::OC_RangeLiteralContext::OC_RangeLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_RangeLiteralContext::STAR() {
  return getToken(CypherParser::STAR, 0);
}

std::vector<CypherParser::OC_IntegerLiteralContext *> CypherParser::OC_RangeLiteralContext::oC_IntegerLiteral() {
  return getRuleContexts<CypherParser::OC_IntegerLiteralContext>();
}

CypherParser::OC_IntegerLiteralContext* CypherParser::OC_RangeLiteralContext::oC_IntegerLiteral(size_t i) {
  return getRuleContext<CypherParser::OC_IntegerLiteralContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_RangeLiteralContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_RangeLiteralContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_RangeLiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_RangeLiteral;
}


CypherParser::OC_RangeLiteralContext* CypherParser::oC_RangeLiteral() {
  OC_RangeLiteralContext *_localctx = _tracker.createInstance<OC_RangeLiteralContext>(_ctx, getState());
  enterRule(_localctx, 128, CypherParser::RuleOC_RangeLiteral);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1058);
    match(CypherParser::STAR);
    setState(1060);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1059);
      match(CypherParser::SP);
    }
    setState(1062);
    oC_IntegerLiteral();
    setState(1064);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1063);
      match(CypherParser::SP);
    }
    setState(1066);
    match(CypherParser::T__11);
    setState(1068);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1067);
      match(CypherParser::SP);
    }
    setState(1070);
    oC_IntegerLiteral();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_LabelNameContext ------------------------------------------------------------------

CypherParser::OC_LabelNameContext::OC_LabelNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SchemaNameContext* CypherParser::OC_LabelNameContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}


size_t CypherParser::OC_LabelNameContext::getRuleIndex() const {
  return CypherParser::RuleOC_LabelName;
}


CypherParser::OC_LabelNameContext* CypherParser::oC_LabelName() {
  OC_LabelNameContext *_localctx = _tracker.createInstance<OC_LabelNameContext>(_ctx, getState());
  enterRule(_localctx, 130, CypherParser::RuleOC_LabelName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1072);
    oC_SchemaName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RelTypeNameContext ------------------------------------------------------------------

CypherParser::OC_RelTypeNameContext::OC_RelTypeNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SchemaNameContext* CypherParser::OC_RelTypeNameContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}


size_t CypherParser::OC_RelTypeNameContext::getRuleIndex() const {
  return CypherParser::RuleOC_RelTypeName;
}


CypherParser::OC_RelTypeNameContext* CypherParser::oC_RelTypeName() {
  OC_RelTypeNameContext *_localctx = _tracker.createInstance<OC_RelTypeNameContext>(_ctx, getState());
  enterRule(_localctx, 132, CypherParser::RuleOC_RelTypeName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1074);
    oC_SchemaName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ExpressionContext ------------------------------------------------------------------

CypherParser::OC_ExpressionContext::OC_ExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_OrExpressionContext* CypherParser::OC_ExpressionContext::oC_OrExpression() {
  return getRuleContext<CypherParser::OC_OrExpressionContext>(0);
}


size_t CypherParser::OC_ExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_Expression;
}


CypherParser::OC_ExpressionContext* CypherParser::oC_Expression() {
  OC_ExpressionContext *_localctx = _tracker.createInstance<OC_ExpressionContext>(_ctx, getState());
  enterRule(_localctx, 134, CypherParser::RuleOC_Expression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1076);
    oC_OrExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_OrExpressionContext ------------------------------------------------------------------

CypherParser::OC_OrExpressionContext::OC_OrExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_XorExpressionContext *> CypherParser::OC_OrExpressionContext::oC_XorExpression() {
  return getRuleContexts<CypherParser::OC_XorExpressionContext>();
}

CypherParser::OC_XorExpressionContext* CypherParser::OC_OrExpressionContext::oC_XorExpression(size_t i) {
  return getRuleContext<CypherParser::OC_XorExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_OrExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_OrExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_OrExpressionContext::OR() {
  return getTokens(CypherParser::OR);
}

tree::TerminalNode* CypherParser::OC_OrExpressionContext::OR(size_t i) {
  return getToken(CypherParser::OR, i);
}


size_t CypherParser::OC_OrExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_OrExpression;
}


CypherParser::OC_OrExpressionContext* CypherParser::oC_OrExpression() {
  OC_OrExpressionContext *_localctx = _tracker.createInstance<OC_OrExpressionContext>(_ctx, getState());
  enterRule(_localctx, 136, CypherParser::RuleOC_OrExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1078);
    oC_XorExpression();
    setState(1085);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 172, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1079);
        match(CypherParser::SP);
        setState(1080);
        match(CypherParser::OR);
        setState(1081);
        match(CypherParser::SP);
        setState(1082);
        oC_XorExpression(); 
      }
      setState(1087);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 172, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_XorExpressionContext ------------------------------------------------------------------

CypherParser::OC_XorExpressionContext::OC_XorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_AndExpressionContext *> CypherParser::OC_XorExpressionContext::oC_AndExpression() {
  return getRuleContexts<CypherParser::OC_AndExpressionContext>();
}

CypherParser::OC_AndExpressionContext* CypherParser::OC_XorExpressionContext::oC_AndExpression(size_t i) {
  return getRuleContext<CypherParser::OC_AndExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_XorExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_XorExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_XorExpressionContext::XOR() {
  return getTokens(CypherParser::XOR);
}

tree::TerminalNode* CypherParser::OC_XorExpressionContext::XOR(size_t i) {
  return getToken(CypherParser::XOR, i);
}


size_t CypherParser::OC_XorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_XorExpression;
}


CypherParser::OC_XorExpressionContext* CypherParser::oC_XorExpression() {
  OC_XorExpressionContext *_localctx = _tracker.createInstance<OC_XorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 138, CypherParser::RuleOC_XorExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1088);
    oC_AndExpression();
    setState(1095);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 173, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1089);
        match(CypherParser::SP);
        setState(1090);
        match(CypherParser::XOR);
        setState(1091);
        match(CypherParser::SP);
        setState(1092);
        oC_AndExpression(); 
      }
      setState(1097);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 173, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_AndExpressionContext ------------------------------------------------------------------

CypherParser::OC_AndExpressionContext::OC_AndExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_NotExpressionContext *> CypherParser::OC_AndExpressionContext::oC_NotExpression() {
  return getRuleContexts<CypherParser::OC_NotExpressionContext>();
}

CypherParser::OC_NotExpressionContext* CypherParser::OC_AndExpressionContext::oC_NotExpression(size_t i) {
  return getRuleContext<CypherParser::OC_NotExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_AndExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_AndExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_AndExpressionContext::AND() {
  return getTokens(CypherParser::AND);
}

tree::TerminalNode* CypherParser::OC_AndExpressionContext::AND(size_t i) {
  return getToken(CypherParser::AND, i);
}


size_t CypherParser::OC_AndExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_AndExpression;
}


CypherParser::OC_AndExpressionContext* CypherParser::oC_AndExpression() {
  OC_AndExpressionContext *_localctx = _tracker.createInstance<OC_AndExpressionContext>(_ctx, getState());
  enterRule(_localctx, 140, CypherParser::RuleOC_AndExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1098);
    oC_NotExpression();
    setState(1105);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 174, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1099);
        match(CypherParser::SP);
        setState(1100);
        match(CypherParser::AND);
        setState(1101);
        match(CypherParser::SP);
        setState(1102);
        oC_NotExpression(); 
      }
      setState(1107);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 174, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_NotExpressionContext ------------------------------------------------------------------

CypherParser::OC_NotExpressionContext::OC_NotExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ComparisonExpressionContext* CypherParser::OC_NotExpressionContext::oC_ComparisonExpression() {
  return getRuleContext<CypherParser::OC_ComparisonExpressionContext>(0);
}

tree::TerminalNode* CypherParser::OC_NotExpressionContext::NOT() {
  return getToken(CypherParser::NOT, 0);
}

tree::TerminalNode* CypherParser::OC_NotExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_NotExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_NotExpression;
}


CypherParser::OC_NotExpressionContext* CypherParser::oC_NotExpression() {
  OC_NotExpressionContext *_localctx = _tracker.createInstance<OC_NotExpressionContext>(_ctx, getState());
  enterRule(_localctx, 142, CypherParser::RuleOC_NotExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1112);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::NOT) {
      setState(1108);
      match(CypherParser::NOT);
      setState(1110);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1109);
        match(CypherParser::SP);
      }
    }
    setState(1114);
    oC_ComparisonExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ComparisonExpressionContext ------------------------------------------------------------------

CypherParser::OC_ComparisonExpressionContext::OC_ComparisonExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_BitwiseOrOperatorExpressionContext *> CypherParser::OC_ComparisonExpressionContext::kU_BitwiseOrOperatorExpression() {
  return getRuleContexts<CypherParser::KU_BitwiseOrOperatorExpressionContext>();
}

CypherParser::KU_BitwiseOrOperatorExpressionContext* CypherParser::OC_ComparisonExpressionContext::kU_BitwiseOrOperatorExpression(size_t i) {
  return getRuleContext<CypherParser::KU_BitwiseOrOperatorExpressionContext>(i);
}

std::vector<CypherParser::KU_ComparisonOperatorContext *> CypherParser::OC_ComparisonExpressionContext::kU_ComparisonOperator() {
  return getRuleContexts<CypherParser::KU_ComparisonOperatorContext>();
}

CypherParser::KU_ComparisonOperatorContext* CypherParser::OC_ComparisonExpressionContext::kU_ComparisonOperator(size_t i) {
  return getRuleContext<CypherParser::KU_ComparisonOperatorContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_ComparisonExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ComparisonExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_ComparisonExpressionContext::INVALID_NOT_EQUAL() {
  return getToken(CypherParser::INVALID_NOT_EQUAL, 0);
}


size_t CypherParser::OC_ComparisonExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_ComparisonExpression;
}


CypherParser::OC_ComparisonExpressionContext* CypherParser::oC_ComparisonExpression() {
  OC_ComparisonExpressionContext *_localctx = _tracker.createInstance<OC_ComparisonExpressionContext>(_ctx, getState());
  enterRule(_localctx, 144, CypherParser::RuleOC_ComparisonExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    setState(1164);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 187, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1116);
      kU_BitwiseOrOperatorExpression();
      setState(1126);
      _errHandler->sync(this);

      switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 179, _ctx)) {
      case 1: {
        setState(1118);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1117);
          match(CypherParser::SP);
        }
        setState(1120);
        kU_ComparisonOperator();
        setState(1122);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1121);
          match(CypherParser::SP);
        }
        setState(1124);
        kU_BitwiseOrOperatorExpression();
        break;
      }

      default:
        break;
      }
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1128);
      kU_BitwiseOrOperatorExpression();

      setState(1130);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1129);
        match(CypherParser::SP);
      }
      setState(1132);
      dynamic_cast<OC_ComparisonExpressionContext *>(_localctx)->invalid_not_equalToken = match(CypherParser::INVALID_NOT_EQUAL);
      setState(1134);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1133);
        match(CypherParser::SP);
      }
      setState(1136);
      kU_BitwiseOrOperatorExpression();
       notifyInvalidNotEqualOperator(dynamic_cast<OC_ComparisonExpressionContext *>(_localctx)->invalid_not_equalToken); 
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(1140);
      kU_BitwiseOrOperatorExpression();
      setState(1142);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1141);
        match(CypherParser::SP);
      }
      setState(1144);
      kU_ComparisonOperator();
      setState(1146);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1145);
        match(CypherParser::SP);
      }
      setState(1148);
      kU_BitwiseOrOperatorExpression();
      setState(1158); 
      _errHandler->sync(this);
      alt = 1;
      do {
        switch (alt) {
          case 1: {
                setState(1150);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == CypherParser::SP) {
                  setState(1149);
                  match(CypherParser::SP);
                }
                setState(1152);
                kU_ComparisonOperator();
                setState(1154);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == CypherParser::SP) {
                  setState(1153);
                  match(CypherParser::SP);
                }
                setState(1156);
                kU_BitwiseOrOperatorExpression();
                break;
              }

        default:
          throw NoViableAltException(this);
        }
        setState(1160); 
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 186, _ctx);
      } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
       notifyNonBinaryComparison(_localctx->start); 
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ComparisonOperatorContext ------------------------------------------------------------------

CypherParser::KU_ComparisonOperatorContext::KU_ComparisonOperatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t CypherParser::KU_ComparisonOperatorContext::getRuleIndex() const {
  return CypherParser::RuleKU_ComparisonOperator;
}


CypherParser::KU_ComparisonOperatorContext* CypherParser::kU_ComparisonOperator() {
  KU_ComparisonOperatorContext *_localctx = _tracker.createInstance<KU_ComparisonOperatorContext>(_ctx, getState());
  enterRule(_localctx, 146, CypherParser::RuleKU_ComparisonOperator);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1166);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << CypherParser::T__4)
      | (1ULL << CypherParser::T__12)
      | (1ULL << CypherParser::T__13)
      | (1ULL << CypherParser::T__14)
      | (1ULL << CypherParser::T__15)
      | (1ULL << CypherParser::T__16))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_BitwiseOrOperatorExpressionContext ------------------------------------------------------------------

CypherParser::KU_BitwiseOrOperatorExpressionContext::KU_BitwiseOrOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_BitwiseAndOperatorExpressionContext *> CypherParser::KU_BitwiseOrOperatorExpressionContext::kU_BitwiseAndOperatorExpression() {
  return getRuleContexts<CypherParser::KU_BitwiseAndOperatorExpressionContext>();
}

CypherParser::KU_BitwiseAndOperatorExpressionContext* CypherParser::KU_BitwiseOrOperatorExpressionContext::kU_BitwiseAndOperatorExpression(size_t i) {
  return getRuleContext<CypherParser::KU_BitwiseAndOperatorExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_BitwiseOrOperatorExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_BitwiseOrOperatorExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_BitwiseOrOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleKU_BitwiseOrOperatorExpression;
}


CypherParser::KU_BitwiseOrOperatorExpressionContext* CypherParser::kU_BitwiseOrOperatorExpression() {
  KU_BitwiseOrOperatorExpressionContext *_localctx = _tracker.createInstance<KU_BitwiseOrOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 148, CypherParser::RuleKU_BitwiseOrOperatorExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1168);
    kU_BitwiseAndOperatorExpression();
    setState(1179);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 190, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1170);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1169);
          match(CypherParser::SP);
        }
        setState(1172);
        match(CypherParser::T__5);
        setState(1174);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1173);
          match(CypherParser::SP);
        }
        setState(1176);
        kU_BitwiseAndOperatorExpression(); 
      }
      setState(1181);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 190, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_BitwiseAndOperatorExpressionContext ------------------------------------------------------------------

CypherParser::KU_BitwiseAndOperatorExpressionContext::KU_BitwiseAndOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::KU_BitShiftOperatorExpressionContext *> CypherParser::KU_BitwiseAndOperatorExpressionContext::kU_BitShiftOperatorExpression() {
  return getRuleContexts<CypherParser::KU_BitShiftOperatorExpressionContext>();
}

CypherParser::KU_BitShiftOperatorExpressionContext* CypherParser::KU_BitwiseAndOperatorExpressionContext::kU_BitShiftOperatorExpression(size_t i) {
  return getRuleContext<CypherParser::KU_BitShiftOperatorExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_BitwiseAndOperatorExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_BitwiseAndOperatorExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_BitwiseAndOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleKU_BitwiseAndOperatorExpression;
}


CypherParser::KU_BitwiseAndOperatorExpressionContext* CypherParser::kU_BitwiseAndOperatorExpression() {
  KU_BitwiseAndOperatorExpressionContext *_localctx = _tracker.createInstance<KU_BitwiseAndOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 150, CypherParser::RuleKU_BitwiseAndOperatorExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1182);
    kU_BitShiftOperatorExpression();
    setState(1193);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 193, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1184);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1183);
          match(CypherParser::SP);
        }
        setState(1186);
        match(CypherParser::T__17);
        setState(1188);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1187);
          match(CypherParser::SP);
        }
        setState(1190);
        kU_BitShiftOperatorExpression(); 
      }
      setState(1195);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 193, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_BitShiftOperatorExpressionContext ------------------------------------------------------------------

CypherParser::KU_BitShiftOperatorExpressionContext::KU_BitShiftOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_AddOrSubtractExpressionContext *> CypherParser::KU_BitShiftOperatorExpressionContext::oC_AddOrSubtractExpression() {
  return getRuleContexts<CypherParser::OC_AddOrSubtractExpressionContext>();
}

CypherParser::OC_AddOrSubtractExpressionContext* CypherParser::KU_BitShiftOperatorExpressionContext::oC_AddOrSubtractExpression(size_t i) {
  return getRuleContext<CypherParser::OC_AddOrSubtractExpressionContext>(i);
}

std::vector<CypherParser::KU_BitShiftOperatorContext *> CypherParser::KU_BitShiftOperatorExpressionContext::kU_BitShiftOperator() {
  return getRuleContexts<CypherParser::KU_BitShiftOperatorContext>();
}

CypherParser::KU_BitShiftOperatorContext* CypherParser::KU_BitShiftOperatorExpressionContext::kU_BitShiftOperator(size_t i) {
  return getRuleContext<CypherParser::KU_BitShiftOperatorContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::KU_BitShiftOperatorExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::KU_BitShiftOperatorExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::KU_BitShiftOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleKU_BitShiftOperatorExpression;
}


CypherParser::KU_BitShiftOperatorExpressionContext* CypherParser::kU_BitShiftOperatorExpression() {
  KU_BitShiftOperatorExpressionContext *_localctx = _tracker.createInstance<KU_BitShiftOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 152, CypherParser::RuleKU_BitShiftOperatorExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1196);
    oC_AddOrSubtractExpression();
    setState(1208);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 196, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1198);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1197);
          match(CypherParser::SP);
        }
        setState(1200);
        kU_BitShiftOperator();
        setState(1202);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1201);
          match(CypherParser::SP);
        }
        setState(1204);
        oC_AddOrSubtractExpression(); 
      }
      setState(1210);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 196, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_BitShiftOperatorContext ------------------------------------------------------------------

CypherParser::KU_BitShiftOperatorContext::KU_BitShiftOperatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t CypherParser::KU_BitShiftOperatorContext::getRuleIndex() const {
  return CypherParser::RuleKU_BitShiftOperator;
}


CypherParser::KU_BitShiftOperatorContext* CypherParser::kU_BitShiftOperator() {
  KU_BitShiftOperatorContext *_localctx = _tracker.createInstance<KU_BitShiftOperatorContext>(_ctx, getState());
  enterRule(_localctx, 154, CypherParser::RuleKU_BitShiftOperator);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1211);
    _la = _input->LA(1);
    if (!(_la == CypherParser::T__18

    || _la == CypherParser::T__19)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_AddOrSubtractExpressionContext ------------------------------------------------------------------

CypherParser::OC_AddOrSubtractExpressionContext::OC_AddOrSubtractExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_MultiplyDivideModuloExpressionContext *> CypherParser::OC_AddOrSubtractExpressionContext::oC_MultiplyDivideModuloExpression() {
  return getRuleContexts<CypherParser::OC_MultiplyDivideModuloExpressionContext>();
}

CypherParser::OC_MultiplyDivideModuloExpressionContext* CypherParser::OC_AddOrSubtractExpressionContext::oC_MultiplyDivideModuloExpression(size_t i) {
  return getRuleContext<CypherParser::OC_MultiplyDivideModuloExpressionContext>(i);
}

std::vector<CypherParser::KU_AddOrSubtractOperatorContext *> CypherParser::OC_AddOrSubtractExpressionContext::kU_AddOrSubtractOperator() {
  return getRuleContexts<CypherParser::KU_AddOrSubtractOperatorContext>();
}

CypherParser::KU_AddOrSubtractOperatorContext* CypherParser::OC_AddOrSubtractExpressionContext::kU_AddOrSubtractOperator(size_t i) {
  return getRuleContext<CypherParser::KU_AddOrSubtractOperatorContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_AddOrSubtractExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_AddOrSubtractExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_AddOrSubtractExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_AddOrSubtractExpression;
}


CypherParser::OC_AddOrSubtractExpressionContext* CypherParser::oC_AddOrSubtractExpression() {
  OC_AddOrSubtractExpressionContext *_localctx = _tracker.createInstance<OC_AddOrSubtractExpressionContext>(_ctx, getState());
  enterRule(_localctx, 156, CypherParser::RuleOC_AddOrSubtractExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1213);
    oC_MultiplyDivideModuloExpression();
    setState(1225);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 199, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1215);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1214);
          match(CypherParser::SP);
        }
        setState(1217);
        kU_AddOrSubtractOperator();
        setState(1219);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1218);
          match(CypherParser::SP);
        }
        setState(1221);
        oC_MultiplyDivideModuloExpression(); 
      }
      setState(1227);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 199, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_AddOrSubtractOperatorContext ------------------------------------------------------------------

CypherParser::KU_AddOrSubtractOperatorContext::KU_AddOrSubtractOperatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_AddOrSubtractOperatorContext::MINUS() {
  return getToken(CypherParser::MINUS, 0);
}


size_t CypherParser::KU_AddOrSubtractOperatorContext::getRuleIndex() const {
  return CypherParser::RuleKU_AddOrSubtractOperator;
}


CypherParser::KU_AddOrSubtractOperatorContext* CypherParser::kU_AddOrSubtractOperator() {
  KU_AddOrSubtractOperatorContext *_localctx = _tracker.createInstance<KU_AddOrSubtractOperatorContext>(_ctx, getState());
  enterRule(_localctx, 158, CypherParser::RuleKU_AddOrSubtractOperator);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1228);
    _la = _input->LA(1);
    if (!(_la == CypherParser::T__20 || _la == CypherParser::MINUS)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_MultiplyDivideModuloExpressionContext ------------------------------------------------------------------

CypherParser::OC_MultiplyDivideModuloExpressionContext::OC_MultiplyDivideModuloExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_PowerOfExpressionContext *> CypherParser::OC_MultiplyDivideModuloExpressionContext::oC_PowerOfExpression() {
  return getRuleContexts<CypherParser::OC_PowerOfExpressionContext>();
}

CypherParser::OC_PowerOfExpressionContext* CypherParser::OC_MultiplyDivideModuloExpressionContext::oC_PowerOfExpression(size_t i) {
  return getRuleContext<CypherParser::OC_PowerOfExpressionContext>(i);
}

std::vector<CypherParser::KU_MultiplyDivideModuloOperatorContext *> CypherParser::OC_MultiplyDivideModuloExpressionContext::kU_MultiplyDivideModuloOperator() {
  return getRuleContexts<CypherParser::KU_MultiplyDivideModuloOperatorContext>();
}

CypherParser::KU_MultiplyDivideModuloOperatorContext* CypherParser::OC_MultiplyDivideModuloExpressionContext::kU_MultiplyDivideModuloOperator(size_t i) {
  return getRuleContext<CypherParser::KU_MultiplyDivideModuloOperatorContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_MultiplyDivideModuloExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_MultiplyDivideModuloExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_MultiplyDivideModuloExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_MultiplyDivideModuloExpression;
}


CypherParser::OC_MultiplyDivideModuloExpressionContext* CypherParser::oC_MultiplyDivideModuloExpression() {
  OC_MultiplyDivideModuloExpressionContext *_localctx = _tracker.createInstance<OC_MultiplyDivideModuloExpressionContext>(_ctx, getState());
  enterRule(_localctx, 160, CypherParser::RuleOC_MultiplyDivideModuloExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1230);
    oC_PowerOfExpression();
    setState(1242);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 202, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1232);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1231);
          match(CypherParser::SP);
        }
        setState(1234);
        kU_MultiplyDivideModuloOperator();
        setState(1236);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1235);
          match(CypherParser::SP);
        }
        setState(1238);
        oC_PowerOfExpression(); 
      }
      setState(1244);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 202, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_MultiplyDivideModuloOperatorContext ------------------------------------------------------------------

CypherParser::KU_MultiplyDivideModuloOperatorContext::KU_MultiplyDivideModuloOperatorContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_MultiplyDivideModuloOperatorContext::STAR() {
  return getToken(CypherParser::STAR, 0);
}


size_t CypherParser::KU_MultiplyDivideModuloOperatorContext::getRuleIndex() const {
  return CypherParser::RuleKU_MultiplyDivideModuloOperator;
}


CypherParser::KU_MultiplyDivideModuloOperatorContext* CypherParser::kU_MultiplyDivideModuloOperator() {
  KU_MultiplyDivideModuloOperatorContext *_localctx = _tracker.createInstance<KU_MultiplyDivideModuloOperatorContext>(_ctx, getState());
  enterRule(_localctx, 162, CypherParser::RuleKU_MultiplyDivideModuloOperator);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1245);
    _la = _input->LA(1);
    if (!(((((_la - 22) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 22)) & ((1ULL << (CypherParser::T__21 - 22))
      | (1ULL << (CypherParser::T__22 - 22))
      | (1ULL << (CypherParser::STAR - 22)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PowerOfExpressionContext ------------------------------------------------------------------

CypherParser::OC_PowerOfExpressionContext::OC_PowerOfExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext *> CypherParser::OC_PowerOfExpressionContext::oC_UnaryAddSubtractOrFactorialExpression() {
  return getRuleContexts<CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext>();
}

CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext* CypherParser::OC_PowerOfExpressionContext::oC_UnaryAddSubtractOrFactorialExpression(size_t i) {
  return getRuleContext<CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_PowerOfExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_PowerOfExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_PowerOfExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_PowerOfExpression;
}


CypherParser::OC_PowerOfExpressionContext* CypherParser::oC_PowerOfExpression() {
  OC_PowerOfExpressionContext *_localctx = _tracker.createInstance<OC_PowerOfExpressionContext>(_ctx, getState());
  enterRule(_localctx, 164, CypherParser::RuleOC_PowerOfExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1247);
    oC_UnaryAddSubtractOrFactorialExpression();
    setState(1258);
    _errHandler->sync(this);
    alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 205, _ctx);
    while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER) {
      if (alt == 1) {
        setState(1249);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1248);
          match(CypherParser::SP);
        }
        setState(1251);
        match(CypherParser::T__23);
        setState(1253);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1252);
          match(CypherParser::SP);
        }
        setState(1255);
        oC_UnaryAddSubtractOrFactorialExpression(); 
      }
      setState(1260);
      _errHandler->sync(this);
      alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 205, _ctx);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_UnaryAddSubtractOrFactorialExpressionContext ------------------------------------------------------------------

CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::OC_UnaryAddSubtractOrFactorialExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_StringListNullOperatorExpressionContext* CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::oC_StringListNullOperatorExpression() {
  return getRuleContext<CypherParser::OC_StringListNullOperatorExpressionContext>(0);
}

tree::TerminalNode* CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::MINUS() {
  return getToken(CypherParser::MINUS, 0);
}

tree::TerminalNode* CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::FACTORIAL() {
  return getToken(CypherParser::FACTORIAL, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_UnaryAddSubtractOrFactorialExpression;
}


CypherParser::OC_UnaryAddSubtractOrFactorialExpressionContext* CypherParser::oC_UnaryAddSubtractOrFactorialExpression() {
  OC_UnaryAddSubtractOrFactorialExpressionContext *_localctx = _tracker.createInstance<OC_UnaryAddSubtractOrFactorialExpressionContext>(_ctx, getState());
  enterRule(_localctx, 166, CypherParser::RuleOC_UnaryAddSubtractOrFactorialExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1265);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::MINUS) {
      setState(1261);
      match(CypherParser::MINUS);
      setState(1263);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1262);
        match(CypherParser::SP);
      }
    }
    setState(1267);
    oC_StringListNullOperatorExpression();
    setState(1272);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 209, _ctx)) {
    case 1: {
      setState(1269);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1268);
        match(CypherParser::SP);
      }
      setState(1271);
      match(CypherParser::FACTORIAL);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_StringListNullOperatorExpressionContext ------------------------------------------------------------------

CypherParser::OC_StringListNullOperatorExpressionContext::OC_StringListNullOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_PropertyOrLabelsExpressionContext* CypherParser::OC_StringListNullOperatorExpressionContext::oC_PropertyOrLabelsExpression() {
  return getRuleContext<CypherParser::OC_PropertyOrLabelsExpressionContext>(0);
}

CypherParser::OC_StringOperatorExpressionContext* CypherParser::OC_StringListNullOperatorExpressionContext::oC_StringOperatorExpression() {
  return getRuleContext<CypherParser::OC_StringOperatorExpressionContext>(0);
}

CypherParser::OC_ListOperatorExpressionContext* CypherParser::OC_StringListNullOperatorExpressionContext::oC_ListOperatorExpression() {
  return getRuleContext<CypherParser::OC_ListOperatorExpressionContext>(0);
}

CypherParser::OC_NullOperatorExpressionContext* CypherParser::OC_StringListNullOperatorExpressionContext::oC_NullOperatorExpression() {
  return getRuleContext<CypherParser::OC_NullOperatorExpressionContext>(0);
}


size_t CypherParser::OC_StringListNullOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_StringListNullOperatorExpression;
}


CypherParser::OC_StringListNullOperatorExpressionContext* CypherParser::oC_StringListNullOperatorExpression() {
  OC_StringListNullOperatorExpressionContext *_localctx = _tracker.createInstance<OC_StringListNullOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 168, CypherParser::RuleOC_StringListNullOperatorExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1274);
    oC_PropertyOrLabelsExpression();
    setState(1278);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 210, _ctx)) {
    case 1: {
      setState(1275);
      oC_StringOperatorExpression();
      break;
    }

    case 2: {
      setState(1276);
      oC_ListOperatorExpression();
      break;
    }

    case 3: {
      setState(1277);
      oC_NullOperatorExpression();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ListOperatorExpressionContext ------------------------------------------------------------------

CypherParser::OC_ListOperatorExpressionContext::OC_ListOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::KU_ListExtractOperatorExpressionContext* CypherParser::OC_ListOperatorExpressionContext::kU_ListExtractOperatorExpression() {
  return getRuleContext<CypherParser::KU_ListExtractOperatorExpressionContext>(0);
}

CypherParser::KU_ListSliceOperatorExpressionContext* CypherParser::OC_ListOperatorExpressionContext::kU_ListSliceOperatorExpression() {
  return getRuleContext<CypherParser::KU_ListSliceOperatorExpressionContext>(0);
}

CypherParser::OC_ListOperatorExpressionContext* CypherParser::OC_ListOperatorExpressionContext::oC_ListOperatorExpression() {
  return getRuleContext<CypherParser::OC_ListOperatorExpressionContext>(0);
}


size_t CypherParser::OC_ListOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_ListOperatorExpression;
}


CypherParser::OC_ListOperatorExpressionContext* CypherParser::oC_ListOperatorExpression() {
  OC_ListOperatorExpressionContext *_localctx = _tracker.createInstance<OC_ListOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 170, CypherParser::RuleOC_ListOperatorExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1282);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 211, _ctx)) {
    case 1: {
      setState(1280);
      kU_ListExtractOperatorExpression();
      break;
    }

    case 2: {
      setState(1281);
      kU_ListSliceOperatorExpression();
      break;
    }

    default:
      break;
    }
    setState(1285);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 212, _ctx)) {
    case 1: {
      setState(1284);
      oC_ListOperatorExpression();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ListExtractOperatorExpressionContext ------------------------------------------------------------------

CypherParser::KU_ListExtractOperatorExpressionContext::KU_ListExtractOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ExpressionContext* CypherParser::KU_ListExtractOperatorExpressionContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

tree::TerminalNode* CypherParser::KU_ListExtractOperatorExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::KU_ListExtractOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleKU_ListExtractOperatorExpression;
}


CypherParser::KU_ListExtractOperatorExpressionContext* CypherParser::kU_ListExtractOperatorExpression() {
  KU_ListExtractOperatorExpressionContext *_localctx = _tracker.createInstance<KU_ListExtractOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 172, CypherParser::RuleKU_ListExtractOperatorExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1288);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1287);
      match(CypherParser::SP);
    }
    setState(1290);
    match(CypherParser::T__6);
    setState(1291);
    oC_Expression();
    setState(1292);
    match(CypherParser::T__7);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- KU_ListSliceOperatorExpressionContext ------------------------------------------------------------------

CypherParser::KU_ListSliceOperatorExpressionContext::KU_ListSliceOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::KU_ListSliceOperatorExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::KU_ListSliceOperatorExpressionContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::KU_ListSliceOperatorExpressionContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}


size_t CypherParser::KU_ListSliceOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleKU_ListSliceOperatorExpression;
}


CypherParser::KU_ListSliceOperatorExpressionContext* CypherParser::kU_ListSliceOperatorExpression() {
  KU_ListSliceOperatorExpressionContext *_localctx = _tracker.createInstance<KU_ListSliceOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 174, CypherParser::RuleKU_ListSliceOperatorExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1295);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1294);
      match(CypherParser::SP);
    }
    setState(1297);
    match(CypherParser::T__6);
    setState(1299);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << CypherParser::T__1)
      | (1ULL << CypherParser::T__6)
      | (1ULL << CypherParser::T__25))) != 0) || ((((_la - 86) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 86)) & ((1ULL << (CypherParser::NOT - 86))
      | (1ULL << (CypherParser::MINUS - 86))
      | (1ULL << (CypherParser::NULL_ - 86))
      | (1ULL << (CypherParser::TRUE - 86))
      | (1ULL << (CypherParser::FALSE - 86))
      | (1ULL << (CypherParser::EXISTS - 86))
      | (1ULL << (CypherParser::CASE - 86))
      | (1ULL << (CypherParser::StringLiteral - 86))
      | (1ULL << (CypherParser::DecimalInteger - 86))
      | (1ULL << (CypherParser::HexLetter - 86))
      | (1ULL << (CypherParser::RegularDecimalReal - 86))
      | (1ULL << (CypherParser::UnescapedSymbolicName - 86))
      | (1ULL << (CypherParser::EscapedSymbolicName - 86)))) != 0)) {
      setState(1298);
      oC_Expression();
    }
    setState(1301);
    match(CypherParser::T__9);
    setState(1303);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << CypherParser::T__1)
      | (1ULL << CypherParser::T__6)
      | (1ULL << CypherParser::T__25))) != 0) || ((((_la - 86) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 86)) & ((1ULL << (CypherParser::NOT - 86))
      | (1ULL << (CypherParser::MINUS - 86))
      | (1ULL << (CypherParser::NULL_ - 86))
      | (1ULL << (CypherParser::TRUE - 86))
      | (1ULL << (CypherParser::FALSE - 86))
      | (1ULL << (CypherParser::EXISTS - 86))
      | (1ULL << (CypherParser::CASE - 86))
      | (1ULL << (CypherParser::StringLiteral - 86))
      | (1ULL << (CypherParser::DecimalInteger - 86))
      | (1ULL << (CypherParser::HexLetter - 86))
      | (1ULL << (CypherParser::RegularDecimalReal - 86))
      | (1ULL << (CypherParser::UnescapedSymbolicName - 86))
      | (1ULL << (CypherParser::EscapedSymbolicName - 86)))) != 0)) {
      setState(1302);
      oC_Expression();
    }
    setState(1305);
    match(CypherParser::T__7);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_StringOperatorExpressionContext ------------------------------------------------------------------

CypherParser::OC_StringOperatorExpressionContext::OC_StringOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_PropertyOrLabelsExpressionContext* CypherParser::OC_StringOperatorExpressionContext::oC_PropertyOrLabelsExpression() {
  return getRuleContext<CypherParser::OC_PropertyOrLabelsExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_StringOperatorExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_StringOperatorExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_StringOperatorExpressionContext::STARTS() {
  return getToken(CypherParser::STARTS, 0);
}

tree::TerminalNode* CypherParser::OC_StringOperatorExpressionContext::WITH() {
  return getToken(CypherParser::WITH, 0);
}

tree::TerminalNode* CypherParser::OC_StringOperatorExpressionContext::ENDS() {
  return getToken(CypherParser::ENDS, 0);
}

tree::TerminalNode* CypherParser::OC_StringOperatorExpressionContext::CONTAINS() {
  return getToken(CypherParser::CONTAINS, 0);
}


size_t CypherParser::OC_StringOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_StringOperatorExpression;
}


CypherParser::OC_StringOperatorExpressionContext* CypherParser::oC_StringOperatorExpression() {
  OC_StringOperatorExpressionContext *_localctx = _tracker.createInstance<OC_StringOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 176, CypherParser::RuleOC_StringOperatorExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1317);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 217, _ctx)) {
    case 1: {
      setState(1307);
      match(CypherParser::SP);
      setState(1308);
      match(CypherParser::STARTS);
      setState(1309);
      match(CypherParser::SP);
      setState(1310);
      match(CypherParser::WITH);
      break;
    }

    case 2: {
      setState(1311);
      match(CypherParser::SP);
      setState(1312);
      match(CypherParser::ENDS);
      setState(1313);
      match(CypherParser::SP);
      setState(1314);
      match(CypherParser::WITH);
      break;
    }

    case 3: {
      setState(1315);
      match(CypherParser::SP);
      setState(1316);
      match(CypherParser::CONTAINS);
      break;
    }

    default:
      break;
    }
    setState(1320);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1319);
      match(CypherParser::SP);
    }
    setState(1322);
    oC_PropertyOrLabelsExpression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_NullOperatorExpressionContext ------------------------------------------------------------------

CypherParser::OC_NullOperatorExpressionContext::OC_NullOperatorExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::OC_NullOperatorExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_NullOperatorExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_NullOperatorExpressionContext::IS() {
  return getToken(CypherParser::IS, 0);
}

tree::TerminalNode* CypherParser::OC_NullOperatorExpressionContext::NULL_() {
  return getToken(CypherParser::NULL_, 0);
}

tree::TerminalNode* CypherParser::OC_NullOperatorExpressionContext::NOT() {
  return getToken(CypherParser::NOT, 0);
}


size_t CypherParser::OC_NullOperatorExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_NullOperatorExpression;
}


CypherParser::OC_NullOperatorExpressionContext* CypherParser::oC_NullOperatorExpression() {
  OC_NullOperatorExpressionContext *_localctx = _tracker.createInstance<OC_NullOperatorExpressionContext>(_ctx, getState());
  enterRule(_localctx, 178, CypherParser::RuleOC_NullOperatorExpression);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1334);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 219, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1324);
      match(CypherParser::SP);
      setState(1325);
      match(CypherParser::IS);
      setState(1326);
      match(CypherParser::SP);
      setState(1327);
      match(CypherParser::NULL_);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1328);
      match(CypherParser::SP);
      setState(1329);
      match(CypherParser::IS);
      setState(1330);
      match(CypherParser::SP);
      setState(1331);
      match(CypherParser::NOT);
      setState(1332);
      match(CypherParser::SP);
      setState(1333);
      match(CypherParser::NULL_);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PropertyOrLabelsExpressionContext ------------------------------------------------------------------

CypherParser::OC_PropertyOrLabelsExpressionContext::OC_PropertyOrLabelsExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_AtomContext* CypherParser::OC_PropertyOrLabelsExpressionContext::oC_Atom() {
  return getRuleContext<CypherParser::OC_AtomContext>(0);
}

CypherParser::OC_PropertyLookupContext* CypherParser::OC_PropertyOrLabelsExpressionContext::oC_PropertyLookup() {
  return getRuleContext<CypherParser::OC_PropertyLookupContext>(0);
}

tree::TerminalNode* CypherParser::OC_PropertyOrLabelsExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_PropertyOrLabelsExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_PropertyOrLabelsExpression;
}


CypherParser::OC_PropertyOrLabelsExpressionContext* CypherParser::oC_PropertyOrLabelsExpression() {
  OC_PropertyOrLabelsExpressionContext *_localctx = _tracker.createInstance<OC_PropertyOrLabelsExpressionContext>(_ctx, getState());
  enterRule(_localctx, 180, CypherParser::RuleOC_PropertyOrLabelsExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1336);
    oC_Atom();
    setState(1341);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 221, _ctx)) {
    case 1: {
      setState(1338);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1337);
        match(CypherParser::SP);
      }
      setState(1340);
      oC_PropertyLookup();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_AtomContext ------------------------------------------------------------------

CypherParser::OC_AtomContext::OC_AtomContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_LiteralContext* CypherParser::OC_AtomContext::oC_Literal() {
  return getRuleContext<CypherParser::OC_LiteralContext>(0);
}

CypherParser::OC_ParameterContext* CypherParser::OC_AtomContext::oC_Parameter() {
  return getRuleContext<CypherParser::OC_ParameterContext>(0);
}

CypherParser::OC_CaseExpressionContext* CypherParser::OC_AtomContext::oC_CaseExpression() {
  return getRuleContext<CypherParser::OC_CaseExpressionContext>(0);
}

CypherParser::OC_ParenthesizedExpressionContext* CypherParser::OC_AtomContext::oC_ParenthesizedExpression() {
  return getRuleContext<CypherParser::OC_ParenthesizedExpressionContext>(0);
}

CypherParser::OC_FunctionInvocationContext* CypherParser::OC_AtomContext::oC_FunctionInvocation() {
  return getRuleContext<CypherParser::OC_FunctionInvocationContext>(0);
}

CypherParser::OC_ExistentialSubqueryContext* CypherParser::OC_AtomContext::oC_ExistentialSubquery() {
  return getRuleContext<CypherParser::OC_ExistentialSubqueryContext>(0);
}

CypherParser::OC_VariableContext* CypherParser::OC_AtomContext::oC_Variable() {
  return getRuleContext<CypherParser::OC_VariableContext>(0);
}


size_t CypherParser::OC_AtomContext::getRuleIndex() const {
  return CypherParser::RuleOC_Atom;
}


CypherParser::OC_AtomContext* CypherParser::oC_Atom() {
  OC_AtomContext *_localctx = _tracker.createInstance<OC_AtomContext>(_ctx, getState());
  enterRule(_localctx, 182, CypherParser::RuleOC_Atom);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1350);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 222, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1343);
      oC_Literal();
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1344);
      oC_Parameter();
      break;
    }

    case 3: {
      enterOuterAlt(_localctx, 3);
      setState(1345);
      oC_CaseExpression();
      break;
    }

    case 4: {
      enterOuterAlt(_localctx, 4);
      setState(1346);
      oC_ParenthesizedExpression();
      break;
    }

    case 5: {
      enterOuterAlt(_localctx, 5);
      setState(1347);
      oC_FunctionInvocation();
      break;
    }

    case 6: {
      enterOuterAlt(_localctx, 6);
      setState(1348);
      oC_ExistentialSubquery();
      break;
    }

    case 7: {
      enterOuterAlt(_localctx, 7);
      setState(1349);
      oC_Variable();
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_LiteralContext ------------------------------------------------------------------

CypherParser::OC_LiteralContext::OC_LiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_NumberLiteralContext* CypherParser::OC_LiteralContext::oC_NumberLiteral() {
  return getRuleContext<CypherParser::OC_NumberLiteralContext>(0);
}

tree::TerminalNode* CypherParser::OC_LiteralContext::StringLiteral() {
  return getToken(CypherParser::StringLiteral, 0);
}

CypherParser::OC_BooleanLiteralContext* CypherParser::OC_LiteralContext::oC_BooleanLiteral() {
  return getRuleContext<CypherParser::OC_BooleanLiteralContext>(0);
}

tree::TerminalNode* CypherParser::OC_LiteralContext::NULL_() {
  return getToken(CypherParser::NULL_, 0);
}

CypherParser::OC_ListLiteralContext* CypherParser::OC_LiteralContext::oC_ListLiteral() {
  return getRuleContext<CypherParser::OC_ListLiteralContext>(0);
}


size_t CypherParser::OC_LiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_Literal;
}


CypherParser::OC_LiteralContext* CypherParser::oC_Literal() {
  OC_LiteralContext *_localctx = _tracker.createInstance<OC_LiteralContext>(_ctx, getState());
  enterRule(_localctx, 184, CypherParser::RuleOC_Literal);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1357);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::DecimalInteger:
      case CypherParser::RegularDecimalReal: {
        enterOuterAlt(_localctx, 1);
        setState(1352);
        oC_NumberLiteral();
        break;
      }

      case CypherParser::StringLiteral: {
        enterOuterAlt(_localctx, 2);
        setState(1353);
        match(CypherParser::StringLiteral);
        break;
      }

      case CypherParser::TRUE:
      case CypherParser::FALSE: {
        enterOuterAlt(_localctx, 3);
        setState(1354);
        oC_BooleanLiteral();
        break;
      }

      case CypherParser::NULL_: {
        enterOuterAlt(_localctx, 4);
        setState(1355);
        match(CypherParser::NULL_);
        break;
      }

      case CypherParser::T__6: {
        enterOuterAlt(_localctx, 5);
        setState(1356);
        oC_ListLiteral();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_BooleanLiteralContext ------------------------------------------------------------------

CypherParser::OC_BooleanLiteralContext::OC_BooleanLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_BooleanLiteralContext::TRUE() {
  return getToken(CypherParser::TRUE, 0);
}

tree::TerminalNode* CypherParser::OC_BooleanLiteralContext::FALSE() {
  return getToken(CypherParser::FALSE, 0);
}


size_t CypherParser::OC_BooleanLiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_BooleanLiteral;
}


CypherParser::OC_BooleanLiteralContext* CypherParser::oC_BooleanLiteral() {
  OC_BooleanLiteralContext *_localctx = _tracker.createInstance<OC_BooleanLiteralContext>(_ctx, getState());
  enterRule(_localctx, 186, CypherParser::RuleOC_BooleanLiteral);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1359);
    _la = _input->LA(1);
    if (!(_la == CypherParser::TRUE

    || _la == CypherParser::FALSE)) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ListLiteralContext ------------------------------------------------------------------

CypherParser::OC_ListLiteralContext::OC_ListLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

std::vector<tree::TerminalNode *> CypherParser::OC_ListLiteralContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ListLiteralContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::OC_ListLiteralContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::OC_ListLiteralContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}


size_t CypherParser::OC_ListLiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_ListLiteral;
}


CypherParser::OC_ListLiteralContext* CypherParser::oC_ListLiteral() {
  OC_ListLiteralContext *_localctx = _tracker.createInstance<OC_ListLiteralContext>(_ctx, getState());
  enterRule(_localctx, 188, CypherParser::RuleOC_ListLiteral);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1361);
    match(CypherParser::T__6);
    setState(1363);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1362);
      match(CypherParser::SP);
    }
    setState(1382);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if ((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << CypherParser::T__1)
      | (1ULL << CypherParser::T__6)
      | (1ULL << CypherParser::T__25))) != 0) || ((((_la - 86) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 86)) & ((1ULL << (CypherParser::NOT - 86))
      | (1ULL << (CypherParser::MINUS - 86))
      | (1ULL << (CypherParser::NULL_ - 86))
      | (1ULL << (CypherParser::TRUE - 86))
      | (1ULL << (CypherParser::FALSE - 86))
      | (1ULL << (CypherParser::EXISTS - 86))
      | (1ULL << (CypherParser::CASE - 86))
      | (1ULL << (CypherParser::StringLiteral - 86))
      | (1ULL << (CypherParser::DecimalInteger - 86))
      | (1ULL << (CypherParser::HexLetter - 86))
      | (1ULL << (CypherParser::RegularDecimalReal - 86))
      | (1ULL << (CypherParser::UnescapedSymbolicName - 86))
      | (1ULL << (CypherParser::EscapedSymbolicName - 86)))) != 0)) {
      setState(1365);
      oC_Expression();
      setState(1367);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1366);
        match(CypherParser::SP);
      }
      setState(1379);
      _errHandler->sync(this);
      _la = _input->LA(1);
      while (_la == CypherParser::T__3) {
        setState(1369);
        match(CypherParser::T__3);
        setState(1371);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1370);
          match(CypherParser::SP);
        }
        setState(1373);
        oC_Expression();
        setState(1375);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1374);
          match(CypherParser::SP);
        }
        setState(1381);
        _errHandler->sync(this);
        _la = _input->LA(1);
      }
    }
    setState(1384);
    match(CypherParser::T__7);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ParenthesizedExpressionContext ------------------------------------------------------------------

CypherParser::OC_ParenthesizedExpressionContext::OC_ParenthesizedExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_ExpressionContext* CypherParser::OC_ParenthesizedExpressionContext::oC_Expression() {
  return getRuleContext<CypherParser::OC_ExpressionContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_ParenthesizedExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ParenthesizedExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_ParenthesizedExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_ParenthesizedExpression;
}


CypherParser::OC_ParenthesizedExpressionContext* CypherParser::oC_ParenthesizedExpression() {
  OC_ParenthesizedExpressionContext *_localctx = _tracker.createInstance<OC_ParenthesizedExpressionContext>(_ctx, getState());
  enterRule(_localctx, 190, CypherParser::RuleOC_ParenthesizedExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1386);
    match(CypherParser::T__1);
    setState(1388);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1387);
      match(CypherParser::SP);
    }
    setState(1390);
    oC_Expression();
    setState(1392);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1391);
      match(CypherParser::SP);
    }
    setState(1394);
    match(CypherParser::T__2);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_FunctionInvocationContext ------------------------------------------------------------------

CypherParser::OC_FunctionInvocationContext::OC_FunctionInvocationContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_FunctionNameContext* CypherParser::OC_FunctionInvocationContext::oC_FunctionName() {
  return getRuleContext<CypherParser::OC_FunctionNameContext>(0);
}

tree::TerminalNode* CypherParser::OC_FunctionInvocationContext::STAR() {
  return getToken(CypherParser::STAR, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_FunctionInvocationContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_FunctionInvocationContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_FunctionInvocationContext::DISTINCT() {
  return getToken(CypherParser::DISTINCT, 0);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::OC_FunctionInvocationContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::OC_FunctionInvocationContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}


size_t CypherParser::OC_FunctionInvocationContext::getRuleIndex() const {
  return CypherParser::RuleOC_FunctionInvocation;
}


CypherParser::OC_FunctionInvocationContext* CypherParser::oC_FunctionInvocation() {
  OC_FunctionInvocationContext *_localctx = _tracker.createInstance<OC_FunctionInvocationContext>(_ctx, getState());
  enterRule(_localctx, 192, CypherParser::RuleOC_FunctionInvocation);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1445);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 244, _ctx)) {
    case 1: {
      enterOuterAlt(_localctx, 1);
      setState(1396);
      oC_FunctionName();
      setState(1398);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1397);
        match(CypherParser::SP);
      }
      setState(1400);
      match(CypherParser::T__1);
      setState(1402);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1401);
        match(CypherParser::SP);
      }
      setState(1404);
      match(CypherParser::STAR);
      setState(1406);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1405);
        match(CypherParser::SP);
      }
      setState(1408);
      match(CypherParser::T__2);
      break;
    }

    case 2: {
      enterOuterAlt(_localctx, 2);
      setState(1410);
      oC_FunctionName();
      setState(1412);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1411);
        match(CypherParser::SP);
      }
      setState(1414);
      match(CypherParser::T__1);
      setState(1416);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1415);
        match(CypherParser::SP);
      }
      setState(1422);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::DISTINCT) {
        setState(1418);
        match(CypherParser::DISTINCT);
        setState(1420);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1419);
          match(CypherParser::SP);
        }
      }
      setState(1441);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if ((((_la & ~ 0x3fULL) == 0) &&
        ((1ULL << _la) & ((1ULL << CypherParser::T__1)
        | (1ULL << CypherParser::T__6)
        | (1ULL << CypherParser::T__25))) != 0) || ((((_la - 86) & ~ 0x3fULL) == 0) &&
        ((1ULL << (_la - 86)) & ((1ULL << (CypherParser::NOT - 86))
        | (1ULL << (CypherParser::MINUS - 86))
        | (1ULL << (CypherParser::NULL_ - 86))
        | (1ULL << (CypherParser::TRUE - 86))
        | (1ULL << (CypherParser::FALSE - 86))
        | (1ULL << (CypherParser::EXISTS - 86))
        | (1ULL << (CypherParser::CASE - 86))
        | (1ULL << (CypherParser::StringLiteral - 86))
        | (1ULL << (CypherParser::DecimalInteger - 86))
        | (1ULL << (CypherParser::HexLetter - 86))
        | (1ULL << (CypherParser::RegularDecimalReal - 86))
        | (1ULL << (CypherParser::UnescapedSymbolicName - 86))
        | (1ULL << (CypherParser::EscapedSymbolicName - 86)))) != 0)) {
        setState(1424);
        oC_Expression();
        setState(1426);
        _errHandler->sync(this);

        _la = _input->LA(1);
        if (_la == CypherParser::SP) {
          setState(1425);
          match(CypherParser::SP);
        }
        setState(1438);
        _errHandler->sync(this);
        _la = _input->LA(1);
        while (_la == CypherParser::T__3) {
          setState(1428);
          match(CypherParser::T__3);
          setState(1430);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(1429);
            match(CypherParser::SP);
          }
          setState(1432);
          oC_Expression();
          setState(1434);
          _errHandler->sync(this);

          _la = _input->LA(1);
          if (_la == CypherParser::SP) {
            setState(1433);
            match(CypherParser::SP);
          }
          setState(1440);
          _errHandler->sync(this);
          _la = _input->LA(1);
        }
      }
      setState(1443);
      match(CypherParser::T__2);
      break;
    }

    default:
      break;
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_FunctionNameContext ------------------------------------------------------------------

CypherParser::OC_FunctionNameContext::OC_FunctionNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::OC_FunctionNameContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}


size_t CypherParser::OC_FunctionNameContext::getRuleIndex() const {
  return CypherParser::RuleOC_FunctionName;
}


CypherParser::OC_FunctionNameContext* CypherParser::oC_FunctionName() {
  OC_FunctionNameContext *_localctx = _tracker.createInstance<OC_FunctionNameContext>(_ctx, getState());
  enterRule(_localctx, 194, CypherParser::RuleOC_FunctionName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1447);
    oC_SymbolicName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ExistentialSubqueryContext ------------------------------------------------------------------

CypherParser::OC_ExistentialSubqueryContext::OC_ExistentialSubqueryContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_ExistentialSubqueryContext::EXISTS() {
  return getToken(CypherParser::EXISTS, 0);
}

tree::TerminalNode* CypherParser::OC_ExistentialSubqueryContext::MATCH() {
  return getToken(CypherParser::MATCH, 0);
}

CypherParser::OC_PatternContext* CypherParser::OC_ExistentialSubqueryContext::oC_Pattern() {
  return getRuleContext<CypherParser::OC_PatternContext>(0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_ExistentialSubqueryContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_ExistentialSubqueryContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

CypherParser::OC_WhereContext* CypherParser::OC_ExistentialSubqueryContext::oC_Where() {
  return getRuleContext<CypherParser::OC_WhereContext>(0);
}


size_t CypherParser::OC_ExistentialSubqueryContext::getRuleIndex() const {
  return CypherParser::RuleOC_ExistentialSubquery;
}


CypherParser::OC_ExistentialSubqueryContext* CypherParser::oC_ExistentialSubquery() {
  OC_ExistentialSubqueryContext *_localctx = _tracker.createInstance<OC_ExistentialSubqueryContext>(_ctx, getState());
  enterRule(_localctx, 196, CypherParser::RuleOC_ExistentialSubquery);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1449);
    match(CypherParser::EXISTS);
    setState(1451);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1450);
      match(CypherParser::SP);
    }
    setState(1453);
    match(CypherParser::T__8);
    setState(1455);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1454);
      match(CypherParser::SP);
    }
    setState(1457);
    match(CypherParser::MATCH);
    setState(1459);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 247, _ctx)) {
    case 1: {
      setState(1458);
      match(CypherParser::SP);
      break;
    }

    default:
      break;
    }
    setState(1461);
    oC_Pattern();
    setState(1466);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 249, _ctx)) {
    case 1: {
      setState(1463);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1462);
        match(CypherParser::SP);
      }
      setState(1465);
      oC_Where();
      break;
    }

    default:
      break;
    }
    setState(1469);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1468);
      match(CypherParser::SP);
    }
    setState(1471);
    match(CypherParser::T__10);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PropertyLookupContext ------------------------------------------------------------------

CypherParser::OC_PropertyLookupContext::OC_PropertyLookupContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_PropertyKeyNameContext* CypherParser::OC_PropertyLookupContext::oC_PropertyKeyName() {
  return getRuleContext<CypherParser::OC_PropertyKeyNameContext>(0);
}

tree::TerminalNode* CypherParser::OC_PropertyLookupContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_PropertyLookupContext::getRuleIndex() const {
  return CypherParser::RuleOC_PropertyLookup;
}


CypherParser::OC_PropertyLookupContext* CypherParser::oC_PropertyLookup() {
  OC_PropertyLookupContext *_localctx = _tracker.createInstance<OC_PropertyLookupContext>(_ctx, getState());
  enterRule(_localctx, 198, CypherParser::RuleOC_PropertyLookup);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1473);
    match(CypherParser::T__24);
    setState(1475);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1474);
      match(CypherParser::SP);
    }

    setState(1477);
    oC_PropertyKeyName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_CaseExpressionContext ------------------------------------------------------------------

CypherParser::OC_CaseExpressionContext::OC_CaseExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_CaseExpressionContext::END() {
  return getToken(CypherParser::END, 0);
}

tree::TerminalNode* CypherParser::OC_CaseExpressionContext::ELSE() {
  return getToken(CypherParser::ELSE, 0);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::OC_CaseExpressionContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::OC_CaseExpressionContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}

std::vector<tree::TerminalNode *> CypherParser::OC_CaseExpressionContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_CaseExpressionContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}

tree::TerminalNode* CypherParser::OC_CaseExpressionContext::CASE() {
  return getToken(CypherParser::CASE, 0);
}

std::vector<CypherParser::OC_CaseAlternativeContext *> CypherParser::OC_CaseExpressionContext::oC_CaseAlternative() {
  return getRuleContexts<CypherParser::OC_CaseAlternativeContext>();
}

CypherParser::OC_CaseAlternativeContext* CypherParser::OC_CaseExpressionContext::oC_CaseAlternative(size_t i) {
  return getRuleContext<CypherParser::OC_CaseAlternativeContext>(i);
}


size_t CypherParser::OC_CaseExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_CaseExpression;
}


CypherParser::OC_CaseExpressionContext* CypherParser::oC_CaseExpression() {
  OC_CaseExpressionContext *_localctx = _tracker.createInstance<OC_CaseExpressionContext>(_ctx, getState());
  enterRule(_localctx, 200, CypherParser::RuleOC_CaseExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    size_t alt;
    enterOuterAlt(_localctx, 1);
    setState(1501);
    _errHandler->sync(this);
    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 257, _ctx)) {
    case 1: {
      setState(1479);
      match(CypherParser::CASE);
      setState(1484); 
      _errHandler->sync(this);
      alt = 1;
      do {
        switch (alt) {
          case 1: {
                setState(1481);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == CypherParser::SP) {
                  setState(1480);
                  match(CypherParser::SP);
                }
                setState(1483);
                oC_CaseAlternative();
                break;
              }

        default:
          throw NoViableAltException(this);
        }
        setState(1486); 
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 253, _ctx);
      } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
      break;
    }

    case 2: {
      setState(1488);
      match(CypherParser::CASE);
      setState(1490);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1489);
        match(CypherParser::SP);
      }
      setState(1492);
      oC_Expression();
      setState(1497); 
      _errHandler->sync(this);
      alt = 1;
      do {
        switch (alt) {
          case 1: {
                setState(1494);
                _errHandler->sync(this);

                _la = _input->LA(1);
                if (_la == CypherParser::SP) {
                  setState(1493);
                  match(CypherParser::SP);
                }
                setState(1496);
                oC_CaseAlternative();
                break;
              }

        default:
          throw NoViableAltException(this);
        }
        setState(1499); 
        _errHandler->sync(this);
        alt = getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 256, _ctx);
      } while (alt != 2 && alt != atn::ATN::INVALID_ALT_NUMBER);
      break;
    }

    default:
      break;
    }
    setState(1511);
    _errHandler->sync(this);

    switch (getInterpreter<atn::ParserATNSimulator>()->adaptivePredict(_input, 260, _ctx)) {
    case 1: {
      setState(1504);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1503);
        match(CypherParser::SP);
      }
      setState(1506);
      match(CypherParser::ELSE);
      setState(1508);
      _errHandler->sync(this);

      _la = _input->LA(1);
      if (_la == CypherParser::SP) {
        setState(1507);
        match(CypherParser::SP);
      }
      setState(1510);
      oC_Expression();
      break;
    }

    default:
      break;
    }
    setState(1514);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1513);
      match(CypherParser::SP);
    }
    setState(1516);
    match(CypherParser::END);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_CaseAlternativeContext ------------------------------------------------------------------

CypherParser::OC_CaseAlternativeContext::OC_CaseAlternativeContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_CaseAlternativeContext::WHEN() {
  return getToken(CypherParser::WHEN, 0);
}

std::vector<CypherParser::OC_ExpressionContext *> CypherParser::OC_CaseAlternativeContext::oC_Expression() {
  return getRuleContexts<CypherParser::OC_ExpressionContext>();
}

CypherParser::OC_ExpressionContext* CypherParser::OC_CaseAlternativeContext::oC_Expression(size_t i) {
  return getRuleContext<CypherParser::OC_ExpressionContext>(i);
}

tree::TerminalNode* CypherParser::OC_CaseAlternativeContext::THEN() {
  return getToken(CypherParser::THEN, 0);
}

std::vector<tree::TerminalNode *> CypherParser::OC_CaseAlternativeContext::SP() {
  return getTokens(CypherParser::SP);
}

tree::TerminalNode* CypherParser::OC_CaseAlternativeContext::SP(size_t i) {
  return getToken(CypherParser::SP, i);
}


size_t CypherParser::OC_CaseAlternativeContext::getRuleIndex() const {
  return CypherParser::RuleOC_CaseAlternative;
}


CypherParser::OC_CaseAlternativeContext* CypherParser::oC_CaseAlternative() {
  OC_CaseAlternativeContext *_localctx = _tracker.createInstance<OC_CaseAlternativeContext>(_ctx, getState());
  enterRule(_localctx, 202, CypherParser::RuleOC_CaseAlternative);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1518);
    match(CypherParser::WHEN);
    setState(1520);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1519);
      match(CypherParser::SP);
    }
    setState(1522);
    oC_Expression();
    setState(1524);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1523);
      match(CypherParser::SP);
    }
    setState(1526);
    match(CypherParser::THEN);
    setState(1528);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1527);
      match(CypherParser::SP);
    }
    setState(1530);
    oC_Expression();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_VariableContext ------------------------------------------------------------------

CypherParser::OC_VariableContext::OC_VariableContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::OC_VariableContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}


size_t CypherParser::OC_VariableContext::getRuleIndex() const {
  return CypherParser::RuleOC_Variable;
}


CypherParser::OC_VariableContext* CypherParser::oC_Variable() {
  OC_VariableContext *_localctx = _tracker.createInstance<OC_VariableContext>(_ctx, getState());
  enterRule(_localctx, 204, CypherParser::RuleOC_Variable);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1532);
    oC_SymbolicName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_NumberLiteralContext ------------------------------------------------------------------

CypherParser::OC_NumberLiteralContext::OC_NumberLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_DoubleLiteralContext* CypherParser::OC_NumberLiteralContext::oC_DoubleLiteral() {
  return getRuleContext<CypherParser::OC_DoubleLiteralContext>(0);
}

CypherParser::OC_IntegerLiteralContext* CypherParser::OC_NumberLiteralContext::oC_IntegerLiteral() {
  return getRuleContext<CypherParser::OC_IntegerLiteralContext>(0);
}


size_t CypherParser::OC_NumberLiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_NumberLiteral;
}


CypherParser::OC_NumberLiteralContext* CypherParser::oC_NumberLiteral() {
  OC_NumberLiteralContext *_localctx = _tracker.createInstance<OC_NumberLiteralContext>(_ctx, getState());
  enterRule(_localctx, 206, CypherParser::RuleOC_NumberLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1536);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::RegularDecimalReal: {
        enterOuterAlt(_localctx, 1);
        setState(1534);
        oC_DoubleLiteral();
        break;
      }

      case CypherParser::DecimalInteger: {
        enterOuterAlt(_localctx, 2);
        setState(1535);
        oC_IntegerLiteral();
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_ParameterContext ------------------------------------------------------------------

CypherParser::OC_ParameterContext::OC_ParameterContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::OC_ParameterContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}

tree::TerminalNode* CypherParser::OC_ParameterContext::DecimalInteger() {
  return getToken(CypherParser::DecimalInteger, 0);
}


size_t CypherParser::OC_ParameterContext::getRuleIndex() const {
  return CypherParser::RuleOC_Parameter;
}


CypherParser::OC_ParameterContext* CypherParser::oC_Parameter() {
  OC_ParameterContext *_localctx = _tracker.createInstance<OC_ParameterContext>(_ctx, getState());
  enterRule(_localctx, 208, CypherParser::RuleOC_Parameter);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1538);
    match(CypherParser::T__25);
    setState(1541);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::HexLetter:
      case CypherParser::UnescapedSymbolicName:
      case CypherParser::EscapedSymbolicName: {
        setState(1539);
        oC_SymbolicName();
        break;
      }

      case CypherParser::DecimalInteger: {
        setState(1540);
        match(CypherParser::DecimalInteger);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PropertyExpressionContext ------------------------------------------------------------------

CypherParser::OC_PropertyExpressionContext::OC_PropertyExpressionContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_AtomContext* CypherParser::OC_PropertyExpressionContext::oC_Atom() {
  return getRuleContext<CypherParser::OC_AtomContext>(0);
}

CypherParser::OC_PropertyLookupContext* CypherParser::OC_PropertyExpressionContext::oC_PropertyLookup() {
  return getRuleContext<CypherParser::OC_PropertyLookupContext>(0);
}

tree::TerminalNode* CypherParser::OC_PropertyExpressionContext::SP() {
  return getToken(CypherParser::SP, 0);
}


size_t CypherParser::OC_PropertyExpressionContext::getRuleIndex() const {
  return CypherParser::RuleOC_PropertyExpression;
}


CypherParser::OC_PropertyExpressionContext* CypherParser::oC_PropertyExpression() {
  OC_PropertyExpressionContext *_localctx = _tracker.createInstance<OC_PropertyExpressionContext>(_ctx, getState());
  enterRule(_localctx, 210, CypherParser::RuleOC_PropertyExpression);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1543);
    oC_Atom();
    setState(1545);
    _errHandler->sync(this);

    _la = _input->LA(1);
    if (_la == CypherParser::SP) {
      setState(1544);
      match(CypherParser::SP);
    }
    setState(1547);
    oC_PropertyLookup();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_PropertyKeyNameContext ------------------------------------------------------------------

CypherParser::OC_PropertyKeyNameContext::OC_PropertyKeyNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SchemaNameContext* CypherParser::OC_PropertyKeyNameContext::oC_SchemaName() {
  return getRuleContext<CypherParser::OC_SchemaNameContext>(0);
}


size_t CypherParser::OC_PropertyKeyNameContext::getRuleIndex() const {
  return CypherParser::RuleOC_PropertyKeyName;
}


CypherParser::OC_PropertyKeyNameContext* CypherParser::oC_PropertyKeyName() {
  OC_PropertyKeyNameContext *_localctx = _tracker.createInstance<OC_PropertyKeyNameContext>(_ctx, getState());
  enterRule(_localctx, 212, CypherParser::RuleOC_PropertyKeyName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1549);
    oC_SchemaName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_IntegerLiteralContext ------------------------------------------------------------------

CypherParser::OC_IntegerLiteralContext::OC_IntegerLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_IntegerLiteralContext::DecimalInteger() {
  return getToken(CypherParser::DecimalInteger, 0);
}


size_t CypherParser::OC_IntegerLiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_IntegerLiteral;
}


CypherParser::OC_IntegerLiteralContext* CypherParser::oC_IntegerLiteral() {
  OC_IntegerLiteralContext *_localctx = _tracker.createInstance<OC_IntegerLiteralContext>(_ctx, getState());
  enterRule(_localctx, 214, CypherParser::RuleOC_IntegerLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1551);
    match(CypherParser::DecimalInteger);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_DoubleLiteralContext ------------------------------------------------------------------

CypherParser::OC_DoubleLiteralContext::OC_DoubleLiteralContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_DoubleLiteralContext::RegularDecimalReal() {
  return getToken(CypherParser::RegularDecimalReal, 0);
}


size_t CypherParser::OC_DoubleLiteralContext::getRuleIndex() const {
  return CypherParser::RuleOC_DoubleLiteral;
}


CypherParser::OC_DoubleLiteralContext* CypherParser::oC_DoubleLiteral() {
  OC_DoubleLiteralContext *_localctx = _tracker.createInstance<OC_DoubleLiteralContext>(_ctx, getState());
  enterRule(_localctx, 216, CypherParser::RuleOC_DoubleLiteral);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1553);
    match(CypherParser::RegularDecimalReal);
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SchemaNameContext ------------------------------------------------------------------

CypherParser::OC_SchemaNameContext::OC_SchemaNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

CypherParser::OC_SymbolicNameContext* CypherParser::OC_SchemaNameContext::oC_SymbolicName() {
  return getRuleContext<CypherParser::OC_SymbolicNameContext>(0);
}


size_t CypherParser::OC_SchemaNameContext::getRuleIndex() const {
  return CypherParser::RuleOC_SchemaName;
}


CypherParser::OC_SchemaNameContext* CypherParser::oC_SchemaName() {
  OC_SchemaNameContext *_localctx = _tracker.createInstance<OC_SchemaNameContext>(_ctx, getState());
  enterRule(_localctx, 218, CypherParser::RuleOC_SchemaName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1555);
    oC_SymbolicName();
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_SymbolicNameContext ------------------------------------------------------------------

CypherParser::OC_SymbolicNameContext::OC_SymbolicNameContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_SymbolicNameContext::UnescapedSymbolicName() {
  return getToken(CypherParser::UnescapedSymbolicName, 0);
}

tree::TerminalNode* CypherParser::OC_SymbolicNameContext::EscapedSymbolicName() {
  return getToken(CypherParser::EscapedSymbolicName, 0);
}

tree::TerminalNode* CypherParser::OC_SymbolicNameContext::HexLetter() {
  return getToken(CypherParser::HexLetter, 0);
}


size_t CypherParser::OC_SymbolicNameContext::getRuleIndex() const {
  return CypherParser::RuleOC_SymbolicName;
}


CypherParser::OC_SymbolicNameContext* CypherParser::oC_SymbolicName() {
  OC_SymbolicNameContext *_localctx = _tracker.createInstance<OC_SymbolicNameContext>(_ctx, getState());
  enterRule(_localctx, 220, CypherParser::RuleOC_SymbolicName);

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    setState(1561);
    _errHandler->sync(this);
    switch (_input->LA(1)) {
      case CypherParser::UnescapedSymbolicName: {
        enterOuterAlt(_localctx, 1);
        setState(1557);
        match(CypherParser::UnescapedSymbolicName);
        break;
      }

      case CypherParser::EscapedSymbolicName: {
        enterOuterAlt(_localctx, 2);
        setState(1558);
        dynamic_cast<OC_SymbolicNameContext *>(_localctx)->escapedsymbolicnameToken = match(CypherParser::EscapedSymbolicName);
        if ((dynamic_cast<OC_SymbolicNameContext *>(_localctx)->escapedsymbolicnameToken != nullptr ? dynamic_cast<OC_SymbolicNameContext *>(_localctx)->escapedsymbolicnameToken->getText() : "") == "``") { notifyEmptyToken(dynamic_cast<OC_SymbolicNameContext *>(_localctx)->escapedsymbolicnameToken); }
        break;
      }

      case CypherParser::HexLetter: {
        enterOuterAlt(_localctx, 3);
        setState(1560);
        match(CypherParser::HexLetter);
        break;
      }

    default:
      throw NoViableAltException(this);
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_LeftArrowHeadContext ------------------------------------------------------------------

CypherParser::OC_LeftArrowHeadContext::OC_LeftArrowHeadContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t CypherParser::OC_LeftArrowHeadContext::getRuleIndex() const {
  return CypherParser::RuleOC_LeftArrowHead;
}


CypherParser::OC_LeftArrowHeadContext* CypherParser::oC_LeftArrowHead() {
  OC_LeftArrowHeadContext *_localctx = _tracker.createInstance<OC_LeftArrowHeadContext>(_ctx, getState());
  enterRule(_localctx, 222, CypherParser::RuleOC_LeftArrowHead);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1563);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << CypherParser::T__13)
      | (1ULL << CypherParser::T__26)
      | (1ULL << CypherParser::T__27)
      | (1ULL << CypherParser::T__28)
      | (1ULL << CypherParser::T__29))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_RightArrowHeadContext ------------------------------------------------------------------

CypherParser::OC_RightArrowHeadContext::OC_RightArrowHeadContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}


size_t CypherParser::OC_RightArrowHeadContext::getRuleIndex() const {
  return CypherParser::RuleOC_RightArrowHead;
}


CypherParser::OC_RightArrowHeadContext* CypherParser::oC_RightArrowHead() {
  OC_RightArrowHeadContext *_localctx = _tracker.createInstance<OC_RightArrowHeadContext>(_ctx, getState());
  enterRule(_localctx, 224, CypherParser::RuleOC_RightArrowHead);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1565);
    _la = _input->LA(1);
    if (!((((_la & ~ 0x3fULL) == 0) &&
      ((1ULL << _la) & ((1ULL << CypherParser::T__15)
      | (1ULL << CypherParser::T__30)
      | (1ULL << CypherParser::T__31)
      | (1ULL << CypherParser::T__32)
      | (1ULL << CypherParser::T__33))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

//----------------- OC_DashContext ------------------------------------------------------------------

CypherParser::OC_DashContext::OC_DashContext(ParserRuleContext *parent, size_t invokingState)
  : ParserRuleContext(parent, invokingState) {
}

tree::TerminalNode* CypherParser::OC_DashContext::MINUS() {
  return getToken(CypherParser::MINUS, 0);
}


size_t CypherParser::OC_DashContext::getRuleIndex() const {
  return CypherParser::RuleOC_Dash;
}


CypherParser::OC_DashContext* CypherParser::oC_Dash() {
  OC_DashContext *_localctx = _tracker.createInstance<OC_DashContext>(_ctx, getState());
  enterRule(_localctx, 226, CypherParser::RuleOC_Dash);
  size_t _la = 0;

#if __cplusplus > 201703L
  auto onExit = finally([=, this] {
#else
  auto onExit = finally([=] {
#endif
    exitRule();
  });
  try {
    enterOuterAlt(_localctx, 1);
    setState(1567);
    _la = _input->LA(1);
    if (!(((((_la - 35) & ~ 0x3fULL) == 0) &&
      ((1ULL << (_la - 35)) & ((1ULL << (CypherParser::T__34 - 35))
      | (1ULL << (CypherParser::T__35 - 35))
      | (1ULL << (CypherParser::T__36 - 35))
      | (1ULL << (CypherParser::T__37 - 35))
      | (1ULL << (CypherParser::T__38 - 35))
      | (1ULL << (CypherParser::T__39 - 35))
      | (1ULL << (CypherParser::T__40 - 35))
      | (1ULL << (CypherParser::T__41 - 35))
      | (1ULL << (CypherParser::T__42 - 35))
      | (1ULL << (CypherParser::T__43 - 35))
      | (1ULL << (CypherParser::T__44 - 35))
      | (1ULL << (CypherParser::MINUS - 35)))) != 0))) {
    _errHandler->recoverInline(this);
    }
    else {
      _errHandler->reportMatch(this);
      consume();
    }
   
  }
  catch (RecognitionException &e) {
    _errHandler->reportError(this, e);
    _localctx->exception = std::current_exception();
    _errHandler->recover(this, _localctx->exception);
  }

  return _localctx;
}

// Static vars and initialization.
std::vector<dfa::DFA> CypherParser::_decisionToDFA;
atn::PredictionContextCache CypherParser::_sharedContextCache;

// We own the ATN which in turn owns the ATN states.
atn::ATN CypherParser::_atn;
std::vector<uint16_t> CypherParser::_serializedATN;

std::vector<std::string> CypherParser::_ruleNames = {
  "oC_Cypher", "kU_CopyCSV", "kU_ParsingOptions", "kU_ParsingOption", "kU_DDL", 
  "kU_CreateNode", "kU_CreateRel", "kU_DropTable", "kU_AlterTable", "kU_AlterOptions", 
  "kU_AddProperty", "kU_DropProperty", "kU_RenameTable", "kU_RenameProperty", 
  "kU_RelConnections", "kU_RelConnection", "kU_NodeLabels", "kU_PropertyDefinitions", 
  "kU_PropertyDefinition", "kU_CreateNodeConstraint", "kU_DataType", "kU_ListIdentifiers", 
  "kU_ListIdentifier", "oC_AnyCypherOption", "oC_Explain", "oC_Profile", 
  "oC_Statement", "oC_Query", "oC_RegularQuery", "oC_Union", "oC_SingleQuery", 
  "oC_SinglePartQuery", "oC_MultiPartQuery", "kU_QueryPart", "oC_UpdatingClause", 
  "oC_ReadingClause", "oC_Match", "oC_Unwind", "oC_Create", "oC_Set", "oC_SetItem", 
  "oC_Delete", "oC_With", "oC_Return", "oC_ProjectionBody", "oC_ProjectionItems", 
  "oC_ProjectionItem", "oC_Order", "oC_Skip", "oC_Limit", "oC_SortItem", 
  "oC_Where", "oC_Pattern", "oC_PatternPart", "oC_AnonymousPatternPart", 
  "oC_PatternElement", "oC_NodePattern", "oC_PatternElementChain", "oC_RelationshipPattern", 
  "oC_RelationshipDetail", "kU_Properties", "oC_RelationshipTypes", "oC_NodeLabels", 
  "oC_NodeLabel", "oC_RangeLiteral", "oC_LabelName", "oC_RelTypeName", "oC_Expression", 
  "oC_OrExpression", "oC_XorExpression", "oC_AndExpression", "oC_NotExpression", 
  "oC_ComparisonExpression", "kU_ComparisonOperator", "kU_BitwiseOrOperatorExpression", 
  "kU_BitwiseAndOperatorExpression", "kU_BitShiftOperatorExpression", "kU_BitShiftOperator", 
  "oC_AddOrSubtractExpression", "kU_AddOrSubtractOperator", "oC_MultiplyDivideModuloExpression", 
  "kU_MultiplyDivideModuloOperator", "oC_PowerOfExpression", "oC_UnaryAddSubtractOrFactorialExpression", 
  "oC_StringListNullOperatorExpression", "oC_ListOperatorExpression", "kU_ListExtractOperatorExpression", 
  "kU_ListSliceOperatorExpression", "oC_StringOperatorExpression", "oC_NullOperatorExpression", 
  "oC_PropertyOrLabelsExpression", "oC_Atom", "oC_Literal", "oC_BooleanLiteral", 
  "oC_ListLiteral", "oC_ParenthesizedExpression", "oC_FunctionInvocation", 
  "oC_FunctionName", "oC_ExistentialSubquery", "oC_PropertyLookup", "oC_CaseExpression", 
  "oC_CaseAlternative", "oC_Variable", "oC_NumberLiteral", "oC_Parameter", 
  "oC_PropertyExpression", "oC_PropertyKeyName", "oC_IntegerLiteral", "oC_DoubleLiteral", 
  "oC_SchemaName", "oC_SymbolicName", "oC_LeftArrowHead", "oC_RightArrowHead", 
  "oC_Dash"
};

std::vector<std::string> CypherParser::_literalNames = {
  "", "';'", "'('", "')'", "','", "'='", "'|'", "'['", "']'", "'{'", "':'", 
  "'}'", "'..'", "'<>'", "'<'", "'<='", "'>'", "'>='", "'&'", "'>>'", "'<<'", 
  "'+'", "'/'", "'%'", "'^'", "'.'", "'$'", "'\u27E8'", "'\u3008'", "'\uFE64'", 
  "'\uFF1C'", "'\u27E9'", "'\u3009'", "'\uFE65'", "'\uFF1E'", "'\u00AD'", 
  "'\u2010'", "'\u2011'", "'\u2012'", "'\u2013'", "'\u2014'", "'\u2015'", 
  "'\u2212'", "'\uFE58'", "'\uFE63'", "'\uFF0D'", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "'*'", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "'!='", "'-'", "'!'", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "'0'"
};

std::vector<std::string> CypherParser::_symbolicNames = {
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", 
  "", "", "", "", "", "", "", "", "", "", "COPY", "FROM", "NODE", "TABLE", 
  "DROP", "ALTER", "DEFAULT", "RENAME", "ADD", "PRIMARY", "KEY", "REL", 
  "TO", "EXPLAIN", "PROFILE", "UNION", "ALL", "OPTIONAL", "MATCH", "UNWIND", 
  "CREATE", "SET", "DELETE", "WITH", "RETURN", "DISTINCT", "STAR", "AS", 
  "ORDER", "BY", "L_SKIP", "LIMIT", "ASCENDING", "ASC", "DESCENDING", "DESC", 
  "WHERE", "OR", "XOR", "AND", "NOT", "INVALID_NOT_EQUAL", "MINUS", "FACTORIAL", 
  "STARTS", "ENDS", "CONTAINS", "IS", "NULL_", "TRUE", "FALSE", "EXISTS", 
  "CASE", "ELSE", "END", "WHEN", "THEN", "StringLiteral", "EscapedChar", 
  "DecimalInteger", "HexLetter", "HexDigit", "Digit", "NonZeroDigit", "NonZeroOctDigit", 
  "ZeroDigit", "RegularDecimalReal", "UnescapedSymbolicName", "IdentifierStart", 
  "IdentifierPart", "EscapedSymbolicName", "SP", "WHITESPACE", "Comment", 
  "Unknown"
};

dfa::Vocabulary CypherParser::_vocabulary(_literalNames, _symbolicNames);

std::vector<std::string> CypherParser::_tokenNames;

CypherParser::Initializer::Initializer() {
	for (size_t i = 0; i < _symbolicNames.size(); ++i) {
		std::string name = _vocabulary.getLiteralName(i);
		if (name.empty()) {
			name = _vocabulary.getSymbolicName(i);
		}

		if (name.empty()) {
			_tokenNames.push_back("<INVALID>");
		} else {
      _tokenNames.push_back(name);
    }
	}

  _serializedATN = {
    0x3, 0x608b, 0xa72a, 0x8133, 0xb9ed, 0x417c, 0x3be7, 0x7786, 0x5964, 
    0x3, 0x7a, 0x624, 0x4, 0x2, 0x9, 0x2, 0x4, 0x3, 0x9, 0x3, 0x4, 0x4, 
    0x9, 0x4, 0x4, 0x5, 0x9, 0x5, 0x4, 0x6, 0x9, 0x6, 0x4, 0x7, 0x9, 0x7, 
    0x4, 0x8, 0x9, 0x8, 0x4, 0x9, 0x9, 0x9, 0x4, 0xa, 0x9, 0xa, 0x4, 0xb, 
    0x9, 0xb, 0x4, 0xc, 0x9, 0xc, 0x4, 0xd, 0x9, 0xd, 0x4, 0xe, 0x9, 0xe, 
    0x4, 0xf, 0x9, 0xf, 0x4, 0x10, 0x9, 0x10, 0x4, 0x11, 0x9, 0x11, 0x4, 
    0x12, 0x9, 0x12, 0x4, 0x13, 0x9, 0x13, 0x4, 0x14, 0x9, 0x14, 0x4, 0x15, 
    0x9, 0x15, 0x4, 0x16, 0x9, 0x16, 0x4, 0x17, 0x9, 0x17, 0x4, 0x18, 0x9, 
    0x18, 0x4, 0x19, 0x9, 0x19, 0x4, 0x1a, 0x9, 0x1a, 0x4, 0x1b, 0x9, 0x1b, 
    0x4, 0x1c, 0x9, 0x1c, 0x4, 0x1d, 0x9, 0x1d, 0x4, 0x1e, 0x9, 0x1e, 0x4, 
    0x1f, 0x9, 0x1f, 0x4, 0x20, 0x9, 0x20, 0x4, 0x21, 0x9, 0x21, 0x4, 0x22, 
    0x9, 0x22, 0x4, 0x23, 0x9, 0x23, 0x4, 0x24, 0x9, 0x24, 0x4, 0x25, 0x9, 
    0x25, 0x4, 0x26, 0x9, 0x26, 0x4, 0x27, 0x9, 0x27, 0x4, 0x28, 0x9, 0x28, 
    0x4, 0x29, 0x9, 0x29, 0x4, 0x2a, 0x9, 0x2a, 0x4, 0x2b, 0x9, 0x2b, 0x4, 
    0x2c, 0x9, 0x2c, 0x4, 0x2d, 0x9, 0x2d, 0x4, 0x2e, 0x9, 0x2e, 0x4, 0x2f, 
    0x9, 0x2f, 0x4, 0x30, 0x9, 0x30, 0x4, 0x31, 0x9, 0x31, 0x4, 0x32, 0x9, 
    0x32, 0x4, 0x33, 0x9, 0x33, 0x4, 0x34, 0x9, 0x34, 0x4, 0x35, 0x9, 0x35, 
    0x4, 0x36, 0x9, 0x36, 0x4, 0x37, 0x9, 0x37, 0x4, 0x38, 0x9, 0x38, 0x4, 
    0x39, 0x9, 0x39, 0x4, 0x3a, 0x9, 0x3a, 0x4, 0x3b, 0x9, 0x3b, 0x4, 0x3c, 
    0x9, 0x3c, 0x4, 0x3d, 0x9, 0x3d, 0x4, 0x3e, 0x9, 0x3e, 0x4, 0x3f, 0x9, 
    0x3f, 0x4, 0x40, 0x9, 0x40, 0x4, 0x41, 0x9, 0x41, 0x4, 0x42, 0x9, 0x42, 
    0x4, 0x43, 0x9, 0x43, 0x4, 0x44, 0x9, 0x44, 0x4, 0x45, 0x9, 0x45, 0x4, 
    0x46, 0x9, 0x46, 0x4, 0x47, 0x9, 0x47, 0x4, 0x48, 0x9, 0x48, 0x4, 0x49, 
    0x9, 0x49, 0x4, 0x4a, 0x9, 0x4a, 0x4, 0x4b, 0x9, 0x4b, 0x4, 0x4c, 0x9, 
    0x4c, 0x4, 0x4d, 0x9, 0x4d, 0x4, 0x4e, 0x9, 0x4e, 0x4, 0x4f, 0x9, 0x4f, 
    0x4, 0x50, 0x9, 0x50, 0x4, 0x51, 0x9, 0x51, 0x4, 0x52, 0x9, 0x52, 0x4, 
    0x53, 0x9, 0x53, 0x4, 0x54, 0x9, 0x54, 0x4, 0x55, 0x9, 0x55, 0x4, 0x56, 
    0x9, 0x56, 0x4, 0x57, 0x9, 0x57, 0x4, 0x58, 0x9, 0x58, 0x4, 0x59, 0x9, 
    0x59, 0x4, 0x5a, 0x9, 0x5a, 0x4, 0x5b, 0x9, 0x5b, 0x4, 0x5c, 0x9, 0x5c, 
    0x4, 0x5d, 0x9, 0x5d, 0x4, 0x5e, 0x9, 0x5e, 0x4, 0x5f, 0x9, 0x5f, 0x4, 
    0x60, 0x9, 0x60, 0x4, 0x61, 0x9, 0x61, 0x4, 0x62, 0x9, 0x62, 0x4, 0x63, 
    0x9, 0x63, 0x4, 0x64, 0x9, 0x64, 0x4, 0x65, 0x9, 0x65, 0x4, 0x66, 0x9, 
    0x66, 0x4, 0x67, 0x9, 0x67, 0x4, 0x68, 0x9, 0x68, 0x4, 0x69, 0x9, 0x69, 
    0x4, 0x6a, 0x9, 0x6a, 0x4, 0x6b, 0x9, 0x6b, 0x4, 0x6c, 0x9, 0x6c, 0x4, 
    0x6d, 0x9, 0x6d, 0x4, 0x6e, 0x9, 0x6e, 0x4, 0x6f, 0x9, 0x6f, 0x4, 0x70, 
    0x9, 0x70, 0x4, 0x71, 0x9, 0x71, 0x4, 0x72, 0x9, 0x72, 0x4, 0x73, 0x9, 
    0x73, 0x3, 0x2, 0x5, 0x2, 0xe8, 0xa, 0x2, 0x3, 0x2, 0x5, 0x2, 0xeb, 
    0xa, 0x2, 0x3, 0x2, 0x5, 0x2, 0xee, 0xa, 0x2, 0x3, 0x2, 0x3, 0x2, 0x3, 
    0x2, 0x5, 0x2, 0xf3, 0xa, 0x2, 0x3, 0x2, 0x5, 0x2, 0xf6, 0xa, 0x2, 0x3, 
    0x2, 0x5, 0x2, 0xf9, 0xa, 0x2, 0x3, 0x2, 0x5, 0x2, 0xfc, 0xa, 0x2, 0x3, 
    0x2, 0x3, 0x2, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 0x3, 
    0x3, 0x3, 0x3, 0x3, 0x3, 0x5, 0x3, 0x108, 0xa, 0x3, 0x3, 0x3, 0x3, 0x3, 
    0x5, 0x3, 0x10c, 0xa, 0x3, 0x3, 0x3, 0x3, 0x3, 0x5, 0x3, 0x110, 0xa, 
    0x3, 0x3, 0x3, 0x3, 0x3, 0x5, 0x3, 0x114, 0xa, 0x3, 0x3, 0x4, 0x3, 0x4, 
    0x5, 0x4, 0x118, 0xa, 0x4, 0x3, 0x4, 0x3, 0x4, 0x5, 0x4, 0x11c, 0xa, 
    0x4, 0x3, 0x4, 0x7, 0x4, 0x11f, 0xa, 0x4, 0xc, 0x4, 0xe, 0x4, 0x122, 
    0xb, 0x4, 0x3, 0x5, 0x3, 0x5, 0x5, 0x5, 0x126, 0xa, 0x5, 0x3, 0x5, 0x3, 
    0x5, 0x5, 0x5, 0x12a, 0xa, 0x5, 0x3, 0x5, 0x3, 0x5, 0x3, 0x6, 0x3, 0x6, 
    0x3, 0x6, 0x3, 0x6, 0x5, 0x6, 0x132, 0xa, 0x6, 0x3, 0x7, 0x3, 0x7, 0x3, 
    0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x13c, 
    0xa, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x140, 0xa, 0x7, 0x3, 0x7, 0x3, 
    0x7, 0x5, 0x7, 0x144, 0xa, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x148, 
    0xa, 0x7, 0x3, 0x7, 0x3, 0x7, 0x3, 0x7, 0x5, 0x7, 0x14d, 0xa, 0x7, 0x3, 
    0x7, 0x3, 0x7, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 
    0x8, 0x3, 0x8, 0x3, 0x8, 0x5, 0x8, 0x159, 0xa, 0x8, 0x3, 0x8, 0x3, 0x8, 
    0x5, 0x8, 0x15d, 0xa, 0x8, 0x3, 0x8, 0x3, 0x8, 0x5, 0x8, 0x161, 0xa, 
    0x8, 0x3, 0x8, 0x3, 0x8, 0x5, 0x8, 0x165, 0xa, 0x8, 0x3, 0x8, 0x3, 0x8, 
    0x5, 0x8, 0x169, 0xa, 0x8, 0x5, 0x8, 0x16b, 0xa, 0x8, 0x3, 0x8, 0x3, 
    0x8, 0x5, 0x8, 0x16f, 0xa, 0x8, 0x3, 0x8, 0x3, 0x8, 0x5, 0x8, 0x173, 
    0xa, 0x8, 0x5, 0x8, 0x175, 0xa, 0x8, 0x3, 0x8, 0x3, 0x8, 0x3, 0x9, 0x3, 
    0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0x9, 0x3, 0xa, 0x3, 0xa, 0x3, 
    0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xa, 0x3, 0xb, 0x3, 
    0xb, 0x3, 0xb, 0x3, 0xb, 0x5, 0xb, 0x18b, 0xa, 0xb, 0x3, 0xc, 0x3, 0xc, 
    0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 0x3, 0xc, 
    0x5, 0xc, 0x196, 0xa, 0xc, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 0xd, 0x3, 
    0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xe, 0x3, 0xf, 0x3, 
    0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 0xf, 0x3, 
    0x10, 0x3, 0x10, 0x5, 0x10, 0x1ac, 0xa, 0x10, 0x3, 0x10, 0x3, 0x10, 
    0x5, 0x10, 0x1b0, 0xa, 0x10, 0x3, 0x10, 0x7, 0x10, 0x1b3, 0xa, 0x10, 
    0xc, 0x10, 0xe, 0x10, 0x1b6, 0xb, 0x10, 0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 
    0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 0x11, 0x3, 0x12, 0x3, 
    0x12, 0x5, 0x12, 0x1c2, 0xa, 0x12, 0x3, 0x12, 0x3, 0x12, 0x5, 0x12, 
    0x1c6, 0xa, 0x12, 0x3, 0x12, 0x7, 0x12, 0x1c9, 0xa, 0x12, 0xc, 0x12, 
    0xe, 0x12, 0x1cc, 0xb, 0x12, 0x3, 0x13, 0x3, 0x13, 0x5, 0x13, 0x1d0, 
    0xa, 0x13, 0x3, 0x13, 0x3, 0x13, 0x5, 0x13, 0x1d4, 0xa, 0x13, 0x3, 0x13, 
    0x7, 0x13, 0x1d7, 0xa, 0x13, 0xc, 0x13, 0xe, 0x13, 0x1da, 0xb, 0x13, 
    0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x14, 0x3, 0x15, 0x3, 0x15, 0x3, 
    0x15, 0x3, 0x15, 0x5, 0x15, 0x1e4, 0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 
    0x5, 0x15, 0x1e8, 0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 0x5, 0x15, 0x1ec, 
    0xa, 0x15, 0x3, 0x15, 0x3, 0x15, 0x3, 0x16, 0x3, 0x16, 0x3, 0x16, 0x3, 
    0x16, 0x5, 0x16, 0x1f4, 0xa, 0x16, 0x3, 0x17, 0x3, 0x17, 0x7, 0x17, 
    0x1f8, 0xa, 0x17, 0xc, 0x17, 0xe, 0x17, 0x1fb, 0xb, 0x17, 0x3, 0x18, 
    0x3, 0x18, 0x3, 0x18, 0x3, 0x19, 0x3, 0x19, 0x5, 0x19, 0x202, 0xa, 0x19, 
    0x3, 0x1a, 0x3, 0x1a, 0x3, 0x1b, 0x3, 0x1b, 0x3, 0x1c, 0x3, 0x1c, 0x3, 
    0x1d, 0x3, 0x1d, 0x3, 0x1e, 0x3, 0x1e, 0x5, 0x1e, 0x20e, 0xa, 0x1e, 
    0x3, 0x1e, 0x7, 0x1e, 0x211, 0xa, 0x1e, 0xc, 0x1e, 0xe, 0x1e, 0x214, 
    0xb, 0x1e, 0x3, 0x1e, 0x3, 0x1e, 0x5, 0x1e, 0x218, 0xa, 0x1e, 0x6, 0x1e, 
    0x21a, 0xa, 0x1e, 0xd, 0x1e, 0xe, 0x1e, 0x21b, 0x3, 0x1e, 0x3, 0x1e, 
    0x3, 0x1e, 0x5, 0x1e, 0x221, 0xa, 0x1e, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 
    0x3, 0x1f, 0x5, 0x1f, 0x227, 0xa, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 0x3, 0x1f, 
    0x5, 0x1f, 0x22c, 0xa, 0x1f, 0x3, 0x1f, 0x5, 0x1f, 0x22f, 0xa, 0x1f, 
    0x3, 0x20, 0x3, 0x20, 0x5, 0x20, 0x233, 0xa, 0x20, 0x3, 0x21, 0x3, 0x21, 
    0x5, 0x21, 0x237, 0xa, 0x21, 0x7, 0x21, 0x239, 0xa, 0x21, 0xc, 0x21, 
    0xe, 0x21, 0x23c, 0xb, 0x21, 0x3, 0x21, 0x3, 0x21, 0x3, 0x21, 0x5, 0x21, 
    0x241, 0xa, 0x21, 0x7, 0x21, 0x243, 0xa, 0x21, 0xc, 0x21, 0xe, 0x21, 
    0x246, 0xb, 0x21, 0x3, 0x21, 0x3, 0x21, 0x5, 0x21, 0x24a, 0xa, 0x21, 
    0x3, 0x21, 0x7, 0x21, 0x24d, 0xa, 0x21, 0xc, 0x21, 0xe, 0x21, 0x250, 
    0xb, 0x21, 0x3, 0x21, 0x5, 0x21, 0x253, 0xa, 0x21, 0x3, 0x21, 0x5, 0x21, 
    0x256, 0xa, 0x21, 0x3, 0x21, 0x3, 0x21, 0x5, 0x21, 0x25a, 0xa, 0x21, 
    0x7, 0x21, 0x25c, 0xa, 0x21, 0xc, 0x21, 0xe, 0x21, 0x25f, 0xb, 0x21, 
    0x3, 0x21, 0x5, 0x21, 0x262, 0xa, 0x21, 0x3, 0x22, 0x3, 0x22, 0x5, 0x22, 
    0x266, 0xa, 0x22, 0x6, 0x22, 0x268, 0xa, 0x22, 0xd, 0x22, 0xe, 0x22, 
    0x269, 0x3, 0x22, 0x3, 0x22, 0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 0x270, 
    0xa, 0x23, 0x7, 0x23, 0x272, 0xa, 0x23, 0xc, 0x23, 0xe, 0x23, 0x275, 
    0xb, 0x23, 0x3, 0x23, 0x3, 0x23, 0x5, 0x23, 0x279, 0xa, 0x23, 0x7, 0x23, 
    0x27b, 0xa, 0x23, 0xc, 0x23, 0xe, 0x23, 0x27e, 0xb, 0x23, 0x3, 0x23, 
    0x3, 0x23, 0x3, 0x24, 0x3, 0x24, 0x3, 0x24, 0x5, 0x24, 0x285, 0xa, 0x24, 
    0x3, 0x25, 0x3, 0x25, 0x5, 0x25, 0x289, 0xa, 0x25, 0x3, 0x26, 0x3, 0x26, 
    0x5, 0x26, 0x28d, 0xa, 0x26, 0x3, 0x26, 0x3, 0x26, 0x5, 0x26, 0x291, 
    0xa, 0x26, 0x3, 0x26, 0x3, 0x26, 0x5, 0x26, 0x295, 0xa, 0x26, 0x3, 0x26, 
    0x5, 0x26, 0x298, 0xa, 0x26, 0x3, 0x27, 0x3, 0x27, 0x5, 0x27, 0x29c, 
    0xa, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 0x27, 0x3, 
    0x27, 0x3, 0x28, 0x3, 0x28, 0x5, 0x28, 0x2a6, 0xa, 0x28, 0x3, 0x28, 
    0x3, 0x28, 0x3, 0x29, 0x3, 0x29, 0x5, 0x29, 0x2ac, 0xa, 0x29, 0x3, 0x29, 
    0x3, 0x29, 0x5, 0x29, 0x2b0, 0xa, 0x29, 0x3, 0x29, 0x3, 0x29, 0x5, 0x29, 
    0x2b4, 0xa, 0x29, 0x3, 0x29, 0x7, 0x29, 0x2b7, 0xa, 0x29, 0xc, 0x29, 
    0xe, 0x29, 0x2ba, 0xb, 0x29, 0x3, 0x2a, 0x3, 0x2a, 0x5, 0x2a, 0x2be, 
    0xa, 0x2a, 0x3, 0x2a, 0x3, 0x2a, 0x5, 0x2a, 0x2c2, 0xa, 0x2a, 0x3, 0x2a, 
    0x3, 0x2a, 0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 0x2c8, 0xa, 0x2b, 0x3, 0x2b, 
    0x3, 0x2b, 0x5, 0x2b, 0x2cc, 0xa, 0x2b, 0x3, 0x2b, 0x3, 0x2b, 0x5, 0x2b, 
    0x2d0, 0xa, 0x2b, 0x3, 0x2b, 0x7, 0x2b, 0x2d3, 0xa, 0x2b, 0xc, 0x2b, 
    0xe, 0x2b, 0x2d6, 0xb, 0x2b, 0x3, 0x2c, 0x3, 0x2c, 0x3, 0x2c, 0x5, 0x2c, 
    0x2db, 0xa, 0x2c, 0x3, 0x2c, 0x5, 0x2c, 0x2de, 0xa, 0x2c, 0x3, 0x2d, 
    0x3, 0x2d, 0x3, 0x2d, 0x3, 0x2e, 0x5, 0x2e, 0x2e4, 0xa, 0x2e, 0x3, 0x2e, 
    0x5, 0x2e, 0x2e7, 0xa, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 
    0x5, 0x2e, 0x2ed, 0xa, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x5, 0x2e, 0x2f1, 
    0xa, 0x2e, 0x3, 0x2e, 0x3, 0x2e, 0x5, 0x2e, 0x2f5, 0xa, 0x2e, 0x3, 0x2f, 
    0x3, 0x2f, 0x5, 0x2f, 0x2f9, 0xa, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x5, 0x2f, 
    0x2fd, 0xa, 0x2f, 0x3, 0x2f, 0x7, 0x2f, 0x300, 0xa, 0x2f, 0xc, 0x2f, 
    0xe, 0x2f, 0x303, 0xb, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x5, 0x2f, 0x307, 
    0xa, 0x2f, 0x3, 0x2f, 0x3, 0x2f, 0x5, 0x2f, 0x30b, 0xa, 0x2f, 0x3, 0x2f, 
    0x7, 0x2f, 0x30e, 0xa, 0x2f, 0xc, 0x2f, 0xe, 0x2f, 0x311, 0xb, 0x2f, 
    0x5, 0x2f, 0x313, 0xa, 0x2f, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 
    0x3, 0x30, 0x3, 0x30, 0x3, 0x30, 0x5, 0x30, 0x31c, 0xa, 0x30, 0x3, 0x31, 
    0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x3, 0x31, 0x5, 
    0x31, 0x325, 0xa, 0x31, 0x3, 0x31, 0x7, 0x31, 0x328, 0xa, 0x31, 0xc, 
    0x31, 0xe, 0x31, 0x32b, 0xb, 0x31, 0x3, 0x32, 0x3, 0x32, 0x3, 0x32, 
    0x3, 0x32, 0x3, 0x33, 0x3, 0x33, 0x3, 0x33, 0x3, 0x33, 0x3, 0x34, 0x3, 
    0x34, 0x5, 0x34, 0x337, 0xa, 0x34, 0x3, 0x34, 0x5, 0x34, 0x33a, 0xa, 
    0x34, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x3, 0x35, 0x3, 0x36, 0x3, 0x36, 
    0x5, 0x36, 0x342, 0xa, 0x36, 0x3, 0x36, 0x3, 0x36, 0x5, 0x36, 0x346, 
    0xa, 0x36, 0x3, 0x36, 0x7, 0x36, 0x349, 0xa, 0x36, 0xc, 0x36, 0xe, 0x36, 
    0x34c, 0xb, 0x36, 0x3, 0x37, 0x3, 0x37, 0x3, 0x38, 0x3, 0x38, 0x3, 0x39, 
    0x3, 0x39, 0x5, 0x39, 0x354, 0xa, 0x39, 0x3, 0x39, 0x7, 0x39, 0x357, 
    0xa, 0x39, 0xc, 0x39, 0xe, 0x39, 0x35a, 0xb, 0x39, 0x3, 0x39, 0x3, 0x39, 
    0x3, 0x39, 0x3, 0x39, 0x5, 0x39, 0x360, 0xa, 0x39, 0x3, 0x3a, 0x3, 0x3a, 
    0x5, 0x3a, 0x364, 0xa, 0x3a, 0x3, 0x3a, 0x3, 0x3a, 0x5, 0x3a, 0x368, 
    0xa, 0x3a, 0x5, 0x3a, 0x36a, 0xa, 0x3a, 0x3, 0x3a, 0x3, 0x3a, 0x5, 0x3a, 
    0x36e, 0xa, 0x3a, 0x5, 0x3a, 0x370, 0xa, 0x3a, 0x3, 0x3a, 0x3, 0x3a, 
    0x5, 0x3a, 0x374, 0xa, 0x3a, 0x5, 0x3a, 0x376, 0xa, 0x3a, 0x3, 0x3a, 
    0x3, 0x3a, 0x5, 0x3a, 0x37a, 0xa, 0x3a, 0x3, 0x3a, 0x3, 0x3a, 0x5, 0x3a, 
    0x37e, 0xa, 0x3a, 0x5, 0x3a, 0x380, 0xa, 0x3a, 0x3, 0x3a, 0x3, 0x3a, 
    0x5, 0x3a, 0x384, 0xa, 0x3a, 0x5, 0x3a, 0x386, 0xa, 0x3a, 0x3, 0x3a, 
    0x3, 0x3a, 0x5, 0x3a, 0x38a, 0xa, 0x3a, 0x5, 0x3a, 0x38c, 0xa, 0x3a, 
    0x3, 0x3a, 0x5, 0x3a, 0x38f, 0xa, 0x3a, 0x3, 0x3b, 0x3, 0x3b, 0x5, 0x3b, 
    0x393, 0xa, 0x3b, 0x3, 0x3b, 0x3, 0x3b, 0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 
    0x399, 0xa, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x39d, 0xa, 0x3c, 
    0x3, 0x3c, 0x5, 0x3c, 0x3a0, 0xa, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x3a3, 
    0xa, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x3a9, 
    0xa, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x3ac, 0xa, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 
    0x3af, 0xa, 0x3c, 0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x3b3, 0xa, 0x3c, 
    0x3, 0x3c, 0x3, 0x3c, 0x5, 0x3c, 0x3b7, 0xa, 0x3c, 0x3, 0x3d, 0x3, 0x3d, 
    0x5, 0x3d, 0x3bb, 0xa, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 0x3bf, 
    0xa, 0x3d, 0x5, 0x3d, 0x3c1, 0xa, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 0x5, 0x3d, 
    0x3c5, 0xa, 0x3d, 0x5, 0x3d, 0x3c7, 0xa, 0x3d, 0x3, 0x3d, 0x3, 0x3d, 
    0x5, 0x3d, 0x3cb, 0xa, 0x3d, 0x5, 0x3d, 0x3cd, 0xa, 0x3d, 0x3, 0x3d, 
    0x3, 0x3d, 0x5, 0x3d, 0x3d1, 0xa, 0x3d, 0x5, 0x3d, 0x3d3, 0xa, 0x3d, 
    0x3, 0x3d, 0x3, 0x3d, 0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x3d9, 0xa, 0x3e, 
    0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x3dd, 0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 
    0x5, 0x3e, 0x3e1, 0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x3e5, 
    0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x3e9, 0xa, 0x3e, 0x3, 0x3e, 
    0x3, 0x3e, 0x5, 0x3e, 0x3ed, 0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 
    0x3f1, 0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x5, 0x3e, 0x3f5, 0xa, 0x3e, 
    0x7, 0x3e, 0x3f7, 0xa, 0x3e, 0xc, 0x3e, 0xe, 0x3e, 0x3fa, 0xb, 0x3e, 
    0x5, 0x3e, 0x3fc, 0xa, 0x3e, 0x3, 0x3e, 0x3, 0x3e, 0x3, 0x3f, 0x3, 0x3f, 
    0x5, 0x3f, 0x402, 0xa, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x5, 0x3f, 0x406, 
    0xa, 0x3f, 0x3, 0x3f, 0x3, 0x3f, 0x5, 0x3f, 0x40a, 0xa, 0x3f, 0x3, 0x3f, 
    0x5, 0x3f, 0x40d, 0xa, 0x3f, 0x3, 0x3f, 0x7, 0x3f, 0x410, 0xa, 0x3f, 
    0xc, 0x3f, 0xe, 0x3f, 0x413, 0xb, 0x3f, 0x3, 0x40, 0x3, 0x40, 0x5, 0x40, 
    0x417, 0xa, 0x40, 0x3, 0x40, 0x7, 0x40, 0x41a, 0xa, 0x40, 0xc, 0x40, 
    0xe, 0x40, 0x41d, 0xb, 0x40, 0x3, 0x41, 0x3, 0x41, 0x5, 0x41, 0x421, 
    0xa, 0x41, 0x3, 0x41, 0x3, 0x41, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x427, 
    0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 0x5, 0x42, 0x42b, 0xa, 0x42, 0x3, 0x42, 
    0x3, 0x42, 0x5, 0x42, 0x42f, 0xa, 0x42, 0x3, 0x42, 0x3, 0x42, 0x3, 0x43, 
    0x3, 0x43, 0x3, 0x44, 0x3, 0x44, 0x3, 0x45, 0x3, 0x45, 0x3, 0x46, 0x3, 
    0x46, 0x3, 0x46, 0x3, 0x46, 0x3, 0x46, 0x7, 0x46, 0x43e, 0xa, 0x46, 
    0xc, 0x46, 0xe, 0x46, 0x441, 0xb, 0x46, 0x3, 0x47, 0x3, 0x47, 0x3, 0x47, 
    0x3, 0x47, 0x3, 0x47, 0x7, 0x47, 0x448, 0xa, 0x47, 0xc, 0x47, 0xe, 0x47, 
    0x44b, 0xb, 0x47, 0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 0x3, 0x48, 
    0x7, 0x48, 0x452, 0xa, 0x48, 0xc, 0x48, 0xe, 0x48, 0x455, 0xb, 0x48, 
    0x3, 0x49, 0x3, 0x49, 0x5, 0x49, 0x459, 0xa, 0x49, 0x5, 0x49, 0x45b, 
    0xa, 0x49, 0x3, 0x49, 0x3, 0x49, 0x3, 0x4a, 0x3, 0x4a, 0x5, 0x4a, 0x461, 
    0xa, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x5, 0x4a, 0x465, 0xa, 0x4a, 0x3, 0x4a, 
    0x3, 0x4a, 0x5, 0x4a, 0x469, 0xa, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x5, 0x4a, 
    0x46d, 0xa, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x5, 0x4a, 0x471, 0xa, 0x4a, 
    0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x5, 
    0x4a, 0x479, 0xa, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x5, 0x4a, 0x47d, 0xa, 
    0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x5, 0x4a, 0x481, 0xa, 0x4a, 0x3, 0x4a, 
    0x3, 0x4a, 0x5, 0x4a, 0x485, 0xa, 0x4a, 0x3, 0x4a, 0x3, 0x4a, 0x6, 0x4a, 
    0x489, 0xa, 0x4a, 0xd, 0x4a, 0xe, 0x4a, 0x48a, 0x3, 0x4a, 0x3, 0x4a, 
    0x5, 0x4a, 0x48f, 0xa, 0x4a, 0x3, 0x4b, 0x3, 0x4b, 0x3, 0x4c, 0x3, 0x4c, 
    0x5, 0x4c, 0x495, 0xa, 0x4c, 0x3, 0x4c, 0x3, 0x4c, 0x5, 0x4c, 0x499, 
    0xa, 0x4c, 0x3, 0x4c, 0x7, 0x4c, 0x49c, 0xa, 0x4c, 0xc, 0x4c, 0xe, 0x4c, 
    0x49f, 0xb, 0x4c, 0x3, 0x4d, 0x3, 0x4d, 0x5, 0x4d, 0x4a3, 0xa, 0x4d, 
    0x3, 0x4d, 0x3, 0x4d, 0x5, 0x4d, 0x4a7, 0xa, 0x4d, 0x3, 0x4d, 0x7, 0x4d, 
    0x4aa, 0xa, 0x4d, 0xc, 0x4d, 0xe, 0x4d, 0x4ad, 0xb, 0x4d, 0x3, 0x4e, 
    0x3, 0x4e, 0x5, 0x4e, 0x4b1, 0xa, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x5, 0x4e, 
    0x4b5, 0xa, 0x4e, 0x3, 0x4e, 0x3, 0x4e, 0x7, 0x4e, 0x4b9, 0xa, 0x4e, 
    0xc, 0x4e, 0xe, 0x4e, 0x4bc, 0xb, 0x4e, 0x3, 0x4f, 0x3, 0x4f, 0x3, 0x50, 
    0x3, 0x50, 0x5, 0x50, 0x4c2, 0xa, 0x50, 0x3, 0x50, 0x3, 0x50, 0x5, 0x50, 
    0x4c6, 0xa, 0x50, 0x3, 0x50, 0x3, 0x50, 0x7, 0x50, 0x4ca, 0xa, 0x50, 
    0xc, 0x50, 0xe, 0x50, 0x4cd, 0xb, 0x50, 0x3, 0x51, 0x3, 0x51, 0x3, 0x52, 
    0x3, 0x52, 0x5, 0x52, 0x4d3, 0xa, 0x52, 0x3, 0x52, 0x3, 0x52, 0x5, 0x52, 
    0x4d7, 0xa, 0x52, 0x3, 0x52, 0x3, 0x52, 0x7, 0x52, 0x4db, 0xa, 0x52, 
    0xc, 0x52, 0xe, 0x52, 0x4de, 0xb, 0x52, 0x3, 0x53, 0x3, 0x53, 0x3, 0x54, 
    0x3, 0x54, 0x5, 0x54, 0x4e4, 0xa, 0x54, 0x3, 0x54, 0x3, 0x54, 0x5, 0x54, 
    0x4e8, 0xa, 0x54, 0x3, 0x54, 0x7, 0x54, 0x4eb, 0xa, 0x54, 0xc, 0x54, 
    0xe, 0x54, 0x4ee, 0xb, 0x54, 0x3, 0x55, 0x3, 0x55, 0x5, 0x55, 0x4f2, 
    0xa, 0x55, 0x5, 0x55, 0x4f4, 0xa, 0x55, 0x3, 0x55, 0x3, 0x55, 0x5, 0x55, 
    0x4f8, 0xa, 0x55, 0x3, 0x55, 0x5, 0x55, 0x4fb, 0xa, 0x55, 0x3, 0x56, 
    0x3, 0x56, 0x3, 0x56, 0x3, 0x56, 0x5, 0x56, 0x501, 0xa, 0x56, 0x3, 0x57, 
    0x3, 0x57, 0x5, 0x57, 0x505, 0xa, 0x57, 0x3, 0x57, 0x5, 0x57, 0x508, 
    0xa, 0x57, 0x3, 0x58, 0x5, 0x58, 0x50b, 0xa, 0x58, 0x3, 0x58, 0x3, 0x58, 
    0x3, 0x58, 0x3, 0x58, 0x3, 0x59, 0x5, 0x59, 0x512, 0xa, 0x59, 0x3, 0x59, 
    0x3, 0x59, 0x5, 0x59, 0x516, 0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 0x5, 0x59, 
    0x51a, 0xa, 0x59, 0x3, 0x59, 0x3, 0x59, 0x3, 0x5a, 0x3, 0x5a, 0x3, 0x5a, 
    0x3, 0x5a, 0x3, 0x5a, 0x3, 0x5a, 0x3, 0x5a, 0x3, 0x5a, 0x3, 0x5a, 0x3, 
    0x5a, 0x5, 0x5a, 0x528, 0xa, 0x5a, 0x3, 0x5a, 0x5, 0x5a, 0x52b, 0xa, 
    0x5a, 0x3, 0x5a, 0x3, 0x5a, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 
    0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x3, 0x5b, 0x5, 
    0x5b, 0x539, 0xa, 0x5b, 0x3, 0x5c, 0x3, 0x5c, 0x5, 0x5c, 0x53d, 0xa, 
    0x5c, 0x3, 0x5c, 0x5, 0x5c, 0x540, 0xa, 0x5c, 0x3, 0x5d, 0x3, 0x5d, 
    0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x3, 0x5d, 0x5, 0x5d, 0x549, 
    0xa, 0x5d, 0x3, 0x5e, 0x3, 0x5e, 0x3, 0x5e, 0x3, 0x5e, 0x3, 0x5e, 0x5, 
    0x5e, 0x550, 0xa, 0x5e, 0x3, 0x5f, 0x3, 0x5f, 0x3, 0x60, 0x3, 0x60, 
    0x5, 0x60, 0x556, 0xa, 0x60, 0x3, 0x60, 0x3, 0x60, 0x5, 0x60, 0x55a, 
    0xa, 0x60, 0x3, 0x60, 0x3, 0x60, 0x5, 0x60, 0x55e, 0xa, 0x60, 0x3, 0x60, 
    0x3, 0x60, 0x5, 0x60, 0x562, 0xa, 0x60, 0x7, 0x60, 0x564, 0xa, 0x60, 
    0xc, 0x60, 0xe, 0x60, 0x567, 0xb, 0x60, 0x5, 0x60, 0x569, 0xa, 0x60, 
    0x3, 0x60, 0x3, 0x60, 0x3, 0x61, 0x3, 0x61, 0x5, 0x61, 0x56f, 0xa, 0x61, 
    0x3, 0x61, 0x3, 0x61, 0x5, 0x61, 0x573, 0xa, 0x61, 0x3, 0x61, 0x3, 0x61, 
    0x3, 0x62, 0x3, 0x62, 0x5, 0x62, 0x579, 0xa, 0x62, 0x3, 0x62, 0x3, 0x62, 
    0x5, 0x62, 0x57d, 0xa, 0x62, 0x3, 0x62, 0x3, 0x62, 0x5, 0x62, 0x581, 
    0xa, 0x62, 0x3, 0x62, 0x3, 0x62, 0x3, 0x62, 0x3, 0x62, 0x5, 0x62, 0x587, 
    0xa, 0x62, 0x3, 0x62, 0x3, 0x62, 0x5, 0x62, 0x58b, 0xa, 0x62, 0x3, 0x62, 
    0x3, 0x62, 0x5, 0x62, 0x58f, 0xa, 0x62, 0x5, 0x62, 0x591, 0xa, 0x62, 
    0x3, 0x62, 0x3, 0x62, 0x5, 0x62, 0x595, 0xa, 0x62, 0x3, 0x62, 0x3, 0x62, 
    0x5, 0x62, 0x599, 0xa, 0x62, 0x3, 0x62, 0x3, 0x62, 0x5, 0x62, 0x59d, 
    0xa, 0x62, 0x7, 0x62, 0x59f, 0xa, 0x62, 0xc, 0x62, 0xe, 0x62, 0x5a2, 
    0xb, 0x62, 0x5, 0x62, 0x5a4, 0xa, 0x62, 0x3, 0x62, 0x3, 0x62, 0x5, 0x62, 
    0x5a8, 0xa, 0x62, 0x3, 0x63, 0x3, 0x63, 0x3, 0x64, 0x3, 0x64, 0x5, 0x64, 
    0x5ae, 0xa, 0x64, 0x3, 0x64, 0x3, 0x64, 0x5, 0x64, 0x5b2, 0xa, 0x64, 
    0x3, 0x64, 0x3, 0x64, 0x5, 0x64, 0x5b6, 0xa, 0x64, 0x3, 0x64, 0x3, 0x64, 
    0x5, 0x64, 0x5ba, 0xa, 0x64, 0x3, 0x64, 0x5, 0x64, 0x5bd, 0xa, 0x64, 
    0x3, 0x64, 0x5, 0x64, 0x5c0, 0xa, 0x64, 0x3, 0x64, 0x3, 0x64, 0x3, 0x65, 
    0x3, 0x65, 0x5, 0x65, 0x5c6, 0xa, 0x65, 0x3, 0x65, 0x3, 0x65, 0x3, 0x66, 
    0x3, 0x66, 0x5, 0x66, 0x5cc, 0xa, 0x66, 0x3, 0x66, 0x6, 0x66, 0x5cf, 
    0xa, 0x66, 0xd, 0x66, 0xe, 0x66, 0x5d0, 0x3, 0x66, 0x3, 0x66, 0x5, 0x66, 
    0x5d5, 0xa, 0x66, 0x3, 0x66, 0x3, 0x66, 0x5, 0x66, 0x5d9, 0xa, 0x66, 
    0x3, 0x66, 0x6, 0x66, 0x5dc, 0xa, 0x66, 0xd, 0x66, 0xe, 0x66, 0x5dd, 
    0x5, 0x66, 0x5e0, 0xa, 0x66, 0x3, 0x66, 0x5, 0x66, 0x5e3, 0xa, 0x66, 
    0x3, 0x66, 0x3, 0x66, 0x5, 0x66, 0x5e7, 0xa, 0x66, 0x3, 0x66, 0x5, 0x66, 
    0x5ea, 0xa, 0x66, 0x3, 0x66, 0x5, 0x66, 0x5ed, 0xa, 0x66, 0x3, 0x66, 
    0x3, 0x66, 0x3, 0x67, 0x3, 0x67, 0x5, 0x67, 0x5f3, 0xa, 0x67, 0x3, 0x67, 
    0x3, 0x67, 0x5, 0x67, 0x5f7, 0xa, 0x67, 0x3, 0x67, 0x3, 0x67, 0x5, 0x67, 
    0x5fb, 0xa, 0x67, 0x3, 0x67, 0x3, 0x67, 0x3, 0x68, 0x3, 0x68, 0x3, 0x69, 
    0x3, 0x69, 0x5, 0x69, 0x603, 0xa, 0x69, 0x3, 0x6a, 0x3, 0x6a, 0x3, 0x6a, 
    0x5, 0x6a, 0x608, 0xa, 0x6a, 0x3, 0x6b, 0x3, 0x6b, 0x5, 0x6b, 0x60c, 
    0xa, 0x6b, 0x3, 0x6b, 0x3, 0x6b, 0x3, 0x6c, 0x3, 0x6c, 0x3, 0x6d, 0x3, 
    0x6d, 0x3, 0x6e, 0x3, 0x6e, 0x3, 0x6f, 0x3, 0x6f, 0x3, 0x70, 0x3, 0x70, 
    0x3, 0x70, 0x3, 0x70, 0x5, 0x70, 0x61c, 0xa, 0x70, 0x3, 0x71, 0x3, 0x71, 
    0x3, 0x72, 0x3, 0x72, 0x3, 0x73, 0x3, 0x73, 0x3, 0x73, 0x2, 0x2, 0x74, 
    0x2, 0x4, 0x6, 0x8, 0xa, 0xc, 0xe, 0x10, 0x12, 0x14, 0x16, 0x18, 0x1a, 
    0x1c, 0x1e, 0x20, 0x22, 0x24, 0x26, 0x28, 0x2a, 0x2c, 0x2e, 0x30, 0x32, 
    0x34, 0x36, 0x38, 0x3a, 0x3c, 0x3e, 0x40, 0x42, 0x44, 0x46, 0x48, 0x4a, 
    0x4c, 0x4e, 0x50, 0x52, 0x54, 0x56, 0x58, 0x5a, 0x5c, 0x5e, 0x60, 0x62, 
    0x64, 0x66, 0x68, 0x6a, 0x6c, 0x6e, 0x70, 0x72, 0x74, 0x76, 0x78, 0x7a, 
    0x7c, 0x7e, 0x80, 0x82, 0x84, 0x86, 0x88, 0x8a, 0x8c, 0x8e, 0x90, 0x92, 
    0x94, 0x96, 0x98, 0x9a, 0x9c, 0x9e, 0xa0, 0xa2, 0xa4, 0xa6, 0xa8, 0xaa, 
    0xac, 0xae, 0xb0, 0xb2, 0xb4, 0xb6, 0xb8, 0xba, 0xbc, 0xbe, 0xc0, 0xc2, 
    0xc4, 0xc6, 0xc8, 0xca, 0xcc, 0xce, 0xd0, 0xd2, 0xd4, 0xd6, 0xd8, 0xda, 
    0xdc, 0xde, 0xe0, 0xe2, 0xe4, 0x2, 0xb, 0x3, 0x2, 0x50, 0x53, 0x4, 0x2, 
    0x7, 0x7, 0xf, 0x13, 0x3, 0x2, 0x15, 0x16, 0x4, 0x2, 0x17, 0x17, 0x5a, 
    0x5a, 0x4, 0x2, 0x18, 0x19, 0x4a, 0x4a, 0x3, 0x2, 0x61, 0x62, 0x4, 0x2, 
    0x10, 0x10, 0x1d, 0x20, 0x4, 0x2, 0x12, 0x12, 0x21, 0x24, 0x4, 0x2, 
    0x25, 0x2f, 0x5a, 0x5a, 0x2, 0x6d2, 0x2, 0xe7, 0x3, 0x2, 0x2, 0x2, 0x4, 
    0xff, 0x3, 0x2, 0x2, 0x2, 0x6, 0x115, 0x3, 0x2, 0x2, 0x2, 0x8, 0x123, 
    0x3, 0x2, 0x2, 0x2, 0xa, 0x131, 0x3, 0x2, 0x2, 0x2, 0xc, 0x133, 0x3, 
    0x2, 0x2, 0x2, 0xe, 0x150, 0x3, 0x2, 0x2, 0x2, 0x10, 0x178, 0x3, 0x2, 
    0x2, 0x2, 0x12, 0x17e, 0x3, 0x2, 0x2, 0x2, 0x14, 0x18a, 0x3, 0x2, 0x2, 
    0x2, 0x16, 0x18c, 0x3, 0x2, 0x2, 0x2, 0x18, 0x197, 0x3, 0x2, 0x2, 0x2, 
    0x1a, 0x19b, 0x3, 0x2, 0x2, 0x2, 0x1c, 0x1a1, 0x3, 0x2, 0x2, 0x2, 0x1e, 
    0x1a9, 0x3, 0x2, 0x2, 0x2, 0x20, 0x1b7, 0x3, 0x2, 0x2, 0x2, 0x22, 0x1bf, 
    0x3, 0x2, 0x2, 0x2, 0x24, 0x1cd, 0x3, 0x2, 0x2, 0x2, 0x26, 0x1db, 0x3, 
    0x2, 0x2, 0x2, 0x28, 0x1df, 0x3, 0x2, 0x2, 0x2, 0x2a, 0x1f3, 0x3, 0x2, 
    0x2, 0x2, 0x2c, 0x1f5, 0x3, 0x2, 0x2, 0x2, 0x2e, 0x1fc, 0x3, 0x2, 0x2, 
    0x2, 0x30, 0x201, 0x3, 0x2, 0x2, 0x2, 0x32, 0x203, 0x3, 0x2, 0x2, 0x2, 
    0x34, 0x205, 0x3, 0x2, 0x2, 0x2, 0x36, 0x207, 0x3, 0x2, 0x2, 0x2, 0x38, 
    0x209, 0x3, 0x2, 0x2, 0x2, 0x3a, 0x220, 0x3, 0x2, 0x2, 0x2, 0x3c, 0x22e, 
    0x3, 0x2, 0x2, 0x2, 0x3e, 0x232, 0x3, 0x2, 0x2, 0x2, 0x40, 0x261, 0x3, 
    0x2, 0x2, 0x2, 0x42, 0x267, 0x3, 0x2, 0x2, 0x2, 0x44, 0x273, 0x3, 0x2, 
    0x2, 0x2, 0x46, 0x284, 0x3, 0x2, 0x2, 0x2, 0x48, 0x288, 0x3, 0x2, 0x2, 
    0x2, 0x4a, 0x28c, 0x3, 0x2, 0x2, 0x2, 0x4c, 0x299, 0x3, 0x2, 0x2, 0x2, 
    0x4e, 0x2a3, 0x3, 0x2, 0x2, 0x2, 0x50, 0x2a9, 0x3, 0x2, 0x2, 0x2, 0x52, 
    0x2bb, 0x3, 0x2, 0x2, 0x2, 0x54, 0x2c5, 0x3, 0x2, 0x2, 0x2, 0x56, 0x2d7, 
    0x3, 0x2, 0x2, 0x2, 0x58, 0x2df, 0x3, 0x2, 0x2, 0x2, 0x5a, 0x2e6, 0x3, 
    0x2, 0x2, 0x2, 0x5c, 0x312, 0x3, 0x2, 0x2, 0x2, 0x5e, 0x31b, 0x3, 0x2, 
    0x2, 0x2, 0x60, 0x31d, 0x3, 0x2, 0x2, 0x2, 0x62, 0x32c, 0x3, 0x2, 0x2, 
    0x2, 0x64, 0x330, 0x3, 0x2, 0x2, 0x2, 0x66, 0x334, 0x3, 0x2, 0x2, 0x2, 
    0x68, 0x33b, 0x3, 0x2, 0x2, 0x2, 0x6a, 0x33f, 0x3, 0x2, 0x2, 0x2, 0x6c, 
    0x34d, 0x3, 0x2, 0x2, 0x2, 0x6e, 0x34f, 0x3, 0x2, 0x2, 0x2, 0x70, 0x35f, 
    0x3, 0x2, 0x2, 0x2, 0x72, 0x38e, 0x3, 0x2, 0x2, 0x2, 0x74, 0x390, 0x3, 
    0x2, 0x2, 0x2, 0x76, 0x3b6, 0x3, 0x2, 0x2, 0x2, 0x78, 0x3b8, 0x3, 0x2, 
    0x2, 0x2, 0x7a, 0x3d6, 0x3, 0x2, 0x2, 0x2, 0x7c, 0x3ff, 0x3, 0x2, 0x2, 
    0x2, 0x7e, 0x414, 0x3, 0x2, 0x2, 0x2, 0x80, 0x41e, 0x3, 0x2, 0x2, 0x2, 
    0x82, 0x424, 0x3, 0x2, 0x2, 0x2, 0x84, 0x432, 0x3, 0x2, 0x2, 0x2, 0x86, 
    0x434, 0x3, 0x2, 0x2, 0x2, 0x88, 0x436, 0x3, 0x2, 0x2, 0x2, 0x8a, 0x438, 
    0x3, 0x2, 0x2, 0x2, 0x8c, 0x442, 0x3, 0x2, 0x2, 0x2, 0x8e, 0x44c, 0x3, 
    0x2, 0x2, 0x2, 0x90, 0x45a, 0x3, 0x2, 0x2, 0x2, 0x92, 0x48e, 0x3, 0x2, 
    0x2, 0x2, 0x94, 0x490, 0x3, 0x2, 0x2, 0x2, 0x96, 0x492, 0x3, 0x2, 0x2, 
    0x2, 0x98, 0x4a0, 0x3, 0x2, 0x2, 0x2, 0x9a, 0x4ae, 0x3, 0x2, 0x2, 0x2, 
    0x9c, 0x4bd, 0x3, 0x2, 0x2, 0x2, 0x9e, 0x4bf, 0x3, 0x2, 0x2, 0x2, 0xa0, 
    0x4ce, 0x3, 0x2, 0x2, 0x2, 0xa2, 0x4d0, 0x3, 0x2, 0x2, 0x2, 0xa4, 0x4df, 
    0x3, 0x2, 0x2, 0x2, 0xa6, 0x4e1, 0x3, 0x2, 0x2, 0x2, 0xa8, 0x4f3, 0x3, 
    0x2, 0x2, 0x2, 0xaa, 0x4fc, 0x3, 0x2, 0x2, 0x2, 0xac, 0x504, 0x3, 0x2, 
    0x2, 0x2, 0xae, 0x50a, 0x3, 0x2, 0x2, 0x2, 0xb0, 0x511, 0x3, 0x2, 0x2, 
    0x2, 0xb2, 0x527, 0x3, 0x2, 0x2, 0x2, 0xb4, 0x538, 0x3, 0x2, 0x2, 0x2, 
    0xb6, 0x53a, 0x3, 0x2, 0x2, 0x2, 0xb8, 0x548, 0x3, 0x2, 0x2, 0x2, 0xba, 
    0x54f, 0x3, 0x2, 0x2, 0x2, 0xbc, 0x551, 0x3, 0x2, 0x2, 0x2, 0xbe, 0x553, 
    0x3, 0x2, 0x2, 0x2, 0xc0, 0x56c, 0x3, 0x2, 0x2, 0x2, 0xc2, 0x5a7, 0x3, 
    0x2, 0x2, 0x2, 0xc4, 0x5a9, 0x3, 0x2, 0x2, 0x2, 0xc6, 0x5ab, 0x3, 0x2, 
    0x2, 0x2, 0xc8, 0x5c3, 0x3, 0x2, 0x2, 0x2, 0xca, 0x5df, 0x3, 0x2, 0x2, 
    0x2, 0xcc, 0x5f0, 0x3, 0x2, 0x2, 0x2, 0xce, 0x5fe, 0x3, 0x2, 0x2, 0x2, 
    0xd0, 0x602, 0x3, 0x2, 0x2, 0x2, 0xd2, 0x604, 0x3, 0x2, 0x2, 0x2, 0xd4, 
    0x609, 0x3, 0x2, 0x2, 0x2, 0xd6, 0x60f, 0x3, 0x2, 0x2, 0x2, 0xd8, 0x611, 
    0x3, 0x2, 0x2, 0x2, 0xda, 0x613, 0x3, 0x2, 0x2, 0x2, 0xdc, 0x615, 0x3, 
    0x2, 0x2, 0x2, 0xde, 0x61b, 0x3, 0x2, 0x2, 0x2, 0xe0, 0x61d, 0x3, 0x2, 
    0x2, 0x2, 0xe2, 0x61f, 0x3, 0x2, 0x2, 0x2, 0xe4, 0x621, 0x3, 0x2, 0x2, 
    0x2, 0xe6, 0xe8, 0x7, 0x77, 0x2, 0x2, 0xe7, 0xe6, 0x3, 0x2, 0x2, 0x2, 
    0xe7, 0xe8, 0x3, 0x2, 0x2, 0x2, 0xe8, 0xea, 0x3, 0x2, 0x2, 0x2, 0xe9, 
    0xeb, 0x5, 0x30, 0x19, 0x2, 0xea, 0xe9, 0x3, 0x2, 0x2, 0x2, 0xea, 0xeb, 
    0x3, 0x2, 0x2, 0x2, 0xeb, 0xed, 0x3, 0x2, 0x2, 0x2, 0xec, 0xee, 0x7, 
    0x77, 0x2, 0x2, 0xed, 0xec, 0x3, 0x2, 0x2, 0x2, 0xed, 0xee, 0x3, 0x2, 
    0x2, 0x2, 0xee, 0xf2, 0x3, 0x2, 0x2, 0x2, 0xef, 0xf3, 0x5, 0x36, 0x1c, 
    0x2, 0xf0, 0xf3, 0x5, 0xa, 0x6, 0x2, 0xf1, 0xf3, 0x5, 0x4, 0x3, 0x2, 
    0xf2, 0xef, 0x3, 0x2, 0x2, 0x2, 0xf2, 0xf0, 0x3, 0x2, 0x2, 0x2, 0xf2, 
    0xf1, 0x3, 0x2, 0x2, 0x2, 0xf3, 0xf8, 0x3, 0x2, 0x2, 0x2, 0xf4, 0xf6, 
    0x7, 0x77, 0x2, 0x2, 0xf5, 0xf4, 0x3, 0x2, 0x2, 0x2, 0xf5, 0xf6, 0x3, 
    0x2, 0x2, 0x2, 0xf6, 0xf7, 0x3, 0x2, 0x2, 0x2, 0xf7, 0xf9, 0x7, 0x3, 
    0x2, 0x2, 0xf8, 0xf5, 0x3, 0x2, 0x2, 0x2, 0xf8, 0xf9, 0x3, 0x2, 0x2, 
    0x2, 0xf9, 0xfb, 0x3, 0x2, 0x2, 0x2, 0xfa, 0xfc, 0x7, 0x77, 0x2, 0x2, 
    0xfb, 0xfa, 0x3, 0x2, 0x2, 0x2, 0xfb, 0xfc, 0x3, 0x2, 0x2, 0x2, 0xfc, 
    0xfd, 0x3, 0x2, 0x2, 0x2, 0xfd, 0xfe, 0x7, 0x2, 0x2, 0x3, 0xfe, 0x3, 
    0x3, 0x2, 0x2, 0x2, 0xff, 0x100, 0x7, 0x30, 0x2, 0x2, 0x100, 0x101, 
    0x7, 0x77, 0x2, 0x2, 0x101, 0x102, 0x5, 0xdc, 0x6f, 0x2, 0x102, 0x103, 
    0x7, 0x77, 0x2, 0x2, 0x103, 0x104, 0x7, 0x31, 0x2, 0x2, 0x104, 0x105, 
    0x7, 0x77, 0x2, 0x2, 0x105, 0x113, 0x7, 0x69, 0x2, 0x2, 0x106, 0x108, 
    0x7, 0x77, 0x2, 0x2, 0x107, 0x106, 0x3, 0x2, 0x2, 0x2, 0x107, 0x108, 
    0x3, 0x2, 0x2, 0x2, 0x108, 0x109, 0x3, 0x2, 0x2, 0x2, 0x109, 0x10b, 
    0x7, 0x4, 0x2, 0x2, 0x10a, 0x10c, 0x7, 0x77, 0x2, 0x2, 0x10b, 0x10a, 
    0x3, 0x2, 0x2, 0x2, 0x10b, 0x10c, 0x3, 0x2, 0x2, 0x2, 0x10c, 0x10d, 
    0x3, 0x2, 0x2, 0x2, 0x10d, 0x10f, 0x5, 0x6, 0x4, 0x2, 0x10e, 0x110, 
    0x7, 0x77, 0x2, 0x2, 0x10f, 0x10e, 0x3, 0x2, 0x2, 0x2, 0x10f, 0x110, 
    0x3, 0x2, 0x2, 0x2, 0x110, 0x111, 0x3, 0x2, 0x2, 0x2, 0x111, 0x112, 
    0x7, 0x5, 0x2, 0x2, 0x112, 0x114, 0x3, 0x2, 0x2, 0x2, 0x113, 0x107, 
    0x3, 0x2, 0x2, 0x2, 0x113, 0x114, 0x3, 0x2, 0x2, 0x2, 0x114, 0x5, 0x3, 
    0x2, 0x2, 0x2, 0x115, 0x120, 0x5, 0x8, 0x5, 0x2, 0x116, 0x118, 0x7, 
    0x77, 0x2, 0x2, 0x117, 0x116, 0x3, 0x2, 0x2, 0x2, 0x117, 0x118, 0x3, 
    0x2, 0x2, 0x2, 0x118, 0x119, 0x3, 0x2, 0x2, 0x2, 0x119, 0x11b, 0x7, 
    0x6, 0x2, 0x2, 0x11a, 0x11c, 0x7, 0x77, 0x2, 0x2, 0x11b, 0x11a, 0x3, 
    0x2, 0x2, 0x2, 0x11b, 0x11c, 0x3, 0x2, 0x2, 0x2, 0x11c, 0x11d, 0x3, 
    0x2, 0x2, 0x2, 0x11d, 0x11f, 0x5, 0x8, 0x5, 0x2, 0x11e, 0x117, 0x3, 
    0x2, 0x2, 0x2, 0x11f, 0x122, 0x3, 0x2, 0x2, 0x2, 0x120, 0x11e, 0x3, 
    0x2, 0x2, 0x2, 0x120, 0x121, 0x3, 0x2, 0x2, 0x2, 0x121, 0x7, 0x3, 0x2, 
    0x2, 0x2, 0x122, 0x120, 0x3, 0x2, 0x2, 0x2, 0x123, 0x125, 0x5, 0xde, 
    0x70, 0x2, 0x124, 0x126, 0x7, 0x77, 0x2, 0x2, 0x125, 0x124, 0x3, 0x2, 
    0x2, 0x2, 0x125, 0x126, 0x3, 0x2, 0x2, 0x2, 0x126, 0x127, 0x3, 0x2, 
    0x2, 0x2, 0x127, 0x129, 0x7, 0x7, 0x2, 0x2, 0x128, 0x12a, 0x7, 0x77, 
    0x2, 0x2, 0x129, 0x128, 0x3, 0x2, 0x2, 0x2, 0x129, 0x12a, 0x3, 0x2, 
    0x2, 0x2, 0x12a, 0x12b, 0x3, 0x2, 0x2, 0x2, 0x12b, 0x12c, 0x5, 0xba, 
    0x5e, 0x2, 0x12c, 0x9, 0x3, 0x2, 0x2, 0x2, 0x12d, 0x132, 0x5, 0xc, 0x7, 
    0x2, 0x12e, 0x132, 0x5, 0xe, 0x8, 0x2, 0x12f, 0x132, 0x5, 0x10, 0x9, 
    0x2, 0x130, 0x132, 0x5, 0x12, 0xa, 0x2, 0x131, 0x12d, 0x3, 0x2, 0x2, 
    0x2, 0x131, 0x12e, 0x3, 0x2, 0x2, 0x2, 0x131, 0x12f, 0x3, 0x2, 0x2, 
    0x2, 0x131, 0x130, 0x3, 0x2, 0x2, 0x2, 0x132, 0xb, 0x3, 0x2, 0x2, 0x2, 
    0x133, 0x134, 0x7, 0x44, 0x2, 0x2, 0x134, 0x135, 0x7, 0x77, 0x2, 0x2, 
    0x135, 0x136, 0x7, 0x32, 0x2, 0x2, 0x136, 0x137, 0x7, 0x77, 0x2, 0x2, 
    0x137, 0x138, 0x7, 0x33, 0x2, 0x2, 0x138, 0x139, 0x7, 0x77, 0x2, 0x2, 
    0x139, 0x13b, 0x5, 0xdc, 0x6f, 0x2, 0x13a, 0x13c, 0x7, 0x77, 0x2, 0x2, 
    0x13b, 0x13a, 0x3, 0x2, 0x2, 0x2, 0x13b, 0x13c, 0x3, 0x2, 0x2, 0x2, 
    0x13c, 0x13d, 0x3, 0x2, 0x2, 0x2, 0x13d, 0x13f, 0x7, 0x4, 0x2, 0x2, 
    0x13e, 0x140, 0x7, 0x77, 0x2, 0x2, 0x13f, 0x13e, 0x3, 0x2, 0x2, 0x2, 
    0x13f, 0x140, 0x3, 0x2, 0x2, 0x2, 0x140, 0x141, 0x3, 0x2, 0x2, 0x2, 
    0x141, 0x143, 0x5, 0x24, 0x13, 0x2, 0x142, 0x144, 0x7, 0x77, 0x2, 0x2, 
    0x143, 0x142, 0x3, 0x2, 0x2, 0x2, 0x143, 0x144, 0x3, 0x2, 0x2, 0x2, 
    0x144, 0x145, 0x3, 0x2, 0x2, 0x2, 0x145, 0x147, 0x7, 0x6, 0x2, 0x2, 
    0x146, 0x148, 0x7, 0x77, 0x2, 0x2, 0x147, 0x146, 0x3, 0x2, 0x2, 0x2, 
    0x147, 0x148, 0x3, 0x2, 0x2, 0x2, 0x148, 0x149, 0x3, 0x2, 0x2, 0x2, 
    0x149, 0x14a, 0x5, 0x28, 0x15, 0x2, 0x14a, 0x14c, 0x3, 0x2, 0x2, 0x2, 
    0x14b, 0x14d, 0x7, 0x77, 0x2, 0x2, 0x14c, 0x14b, 0x3, 0x2, 0x2, 0x2, 
    0x14c, 0x14d, 0x3, 0x2, 0x2, 0x2, 0x14d, 0x14e, 0x3, 0x2, 0x2, 0x2, 
    0x14e, 0x14f, 0x7, 0x5, 0x2, 0x2, 0x14f, 0xd, 0x3, 0x2, 0x2, 0x2, 0x150, 
    0x151, 0x7, 0x44, 0x2, 0x2, 0x151, 0x152, 0x7, 0x77, 0x2, 0x2, 0x152, 
    0x153, 0x7, 0x3b, 0x2, 0x2, 0x153, 0x154, 0x7, 0x77, 0x2, 0x2, 0x154, 
    0x155, 0x7, 0x33, 0x2, 0x2, 0x155, 0x156, 0x7, 0x77, 0x2, 0x2, 0x156, 
    0x158, 0x5, 0xdc, 0x6f, 0x2, 0x157, 0x159, 0x7, 0x77, 0x2, 0x2, 0x158, 
    0x157, 0x3, 0x2, 0x2, 0x2, 0x158, 0x159, 0x3, 0x2, 0x2, 0x2, 0x159, 
    0x15a, 0x3, 0x2, 0x2, 0x2, 0x15a, 0x15c, 0x7, 0x4, 0x2, 0x2, 0x15b, 
    0x15d, 0x7, 0x77, 0x2, 0x2, 0x15c, 0x15b, 0x3, 0x2, 0x2, 0x2, 0x15c, 
    0x15d, 0x3, 0x2, 0x2, 0x2, 0x15d, 0x15e, 0x3, 0x2, 0x2, 0x2, 0x15e, 
    0x160, 0x5, 0x1e, 0x10, 0x2, 0x15f, 0x161, 0x7, 0x77, 0x2, 0x2, 0x160, 
    0x15f, 0x3, 0x2, 0x2, 0x2, 0x160, 0x161, 0x3, 0x2, 0x2, 0x2, 0x161, 
    0x16a, 0x3, 0x2, 0x2, 0x2, 0x162, 0x164, 0x7, 0x6, 0x2, 0x2, 0x163, 
    0x165, 0x7, 0x77, 0x2, 0x2, 0x164, 0x163, 0x3, 0x2, 0x2, 0x2, 0x164, 
    0x165, 0x3, 0x2, 0x2, 0x2, 0x165, 0x166, 0x3, 0x2, 0x2, 0x2, 0x166, 
    0x168, 0x5, 0x24, 0x13, 0x2, 0x167, 0x169, 0x7, 0x77, 0x2, 0x2, 0x168, 
    0x167, 0x3, 0x2, 0x2, 0x2, 0x168, 0x169, 0x3, 0x2, 0x2, 0x2, 0x169, 
    0x16b, 0x3, 0x2, 0x2, 0x2, 0x16a, 0x162, 0x3, 0x2, 0x2, 0x2, 0x16a, 
    0x16b, 0x3, 0x2, 0x2, 0x2, 0x16b, 0x174, 0x3, 0x2, 0x2, 0x2, 0x16c, 
    0x16e, 0x7, 0x6, 0x2, 0x2, 0x16d, 0x16f, 0x7, 0x77, 0x2, 0x2, 0x16e, 
    0x16d, 0x3, 0x2, 0x2, 0x2, 0x16e, 0x16f, 0x3, 0x2, 0x2, 0x2, 0x16f, 
    0x170, 0x3, 0x2, 0x2, 0x2, 0x170, 0x172, 0x5, 0xde, 0x70, 0x2, 0x171, 
    0x173, 0x7, 0x77, 0x2, 0x2, 0x172, 0x171, 0x3, 0x2, 0x2, 0x2, 0x172, 
    0x173, 0x3, 0x2, 0x2, 0x2, 0x173, 0x175, 0x3, 0x2, 0x2, 0x2, 0x174, 
    0x16c, 0x3, 0x2, 0x2, 0x2, 0x174, 0x175, 0x3, 0x2, 0x2, 0x2, 0x175, 
    0x176, 0x3, 0x2, 0x2, 0x2, 0x176, 0x177, 0x7, 0x5, 0x2, 0x2, 0x177, 
    0xf, 0x3, 0x2, 0x2, 0x2, 0x178, 0x179, 0x7, 0x34, 0x2, 0x2, 0x179, 0x17a, 
    0x7, 0x77, 0x2, 0x2, 0x17a, 0x17b, 0x7, 0x33, 0x2, 0x2, 0x17b, 0x17c, 
    0x7, 0x77, 0x2, 0x2, 0x17c, 0x17d, 0x5, 0xdc, 0x6f, 0x2, 0x17d, 0x11, 
    0x3, 0x2, 0x2, 0x2, 0x17e, 0x17f, 0x7, 0x35, 0x2, 0x2, 0x17f, 0x180, 
    0x7, 0x77, 0x2, 0x2, 0x180, 0x181, 0x7, 0x33, 0x2, 0x2, 0x181, 0x182, 
    0x7, 0x77, 0x2, 0x2, 0x182, 0x183, 0x5, 0xdc, 0x6f, 0x2, 0x183, 0x184, 
    0x7, 0x77, 0x2, 0x2, 0x184, 0x185, 0x5, 0x14, 0xb, 0x2, 0x185, 0x13, 
    0x3, 0x2, 0x2, 0x2, 0x186, 0x18b, 0x5, 0x16, 0xc, 0x2, 0x187, 0x18b, 
    0x5, 0x18, 0xd, 0x2, 0x188, 0x18b, 0x5, 0x1a, 0xe, 0x2, 0x189, 0x18b, 
    0x5, 0x1c, 0xf, 0x2, 0x18a, 0x186, 0x3, 0x2, 0x2, 0x2, 0x18a, 0x187, 
    0x3, 0x2, 0x2, 0x2, 0x18a, 0x188, 0x3, 0x2, 0x2, 0x2, 0x18a, 0x189, 
    0x3, 0x2, 0x2, 0x2, 0x18b, 0x15, 0x3, 0x2, 0x2, 0x2, 0x18c, 0x18d, 0x7, 
    0x38, 0x2, 0x2, 0x18d, 0x18e, 0x7, 0x77, 0x2, 0x2, 0x18e, 0x18f, 0x5, 
    0xd6, 0x6c, 0x2, 0x18f, 0x190, 0x7, 0x77, 0x2, 0x2, 0x190, 0x195, 0x5, 
    0x2a, 0x16, 0x2, 0x191, 0x192, 0x7, 0x77, 0x2, 0x2, 0x192, 0x193, 0x7, 
    0x36, 0x2, 0x2, 0x193, 0x194, 0x7, 0x77, 0x2, 0x2, 0x194, 0x196, 0x5, 
    0x88, 0x45, 0x2, 0x195, 0x191, 0x3, 0x2, 0x2, 0x2, 0x195, 0x196, 0x3, 
    0x2, 0x2, 0x2, 0x196, 0x17, 0x3, 0x2, 0x2, 0x2, 0x197, 0x198, 0x7, 0x34, 
    0x2, 0x2, 0x198, 0x199, 0x7, 0x77, 0x2, 0x2, 0x199, 0x19a, 0x5, 0xd6, 
    0x6c, 0x2, 0x19a, 0x19, 0x3, 0x2, 0x2, 0x2, 0x19b, 0x19c, 0x7, 0x37, 
    0x2, 0x2, 0x19c, 0x19d, 0x7, 0x77, 0x2, 0x2, 0x19d, 0x19e, 0x7, 0x3c, 
    0x2, 0x2, 0x19e, 0x19f, 0x7, 0x77, 0x2, 0x2, 0x19f, 0x1a0, 0x5, 0xdc, 
    0x6f, 0x2, 0x1a0, 0x1b, 0x3, 0x2, 0x2, 0x2, 0x1a1, 0x1a2, 0x7, 0x37, 
    0x2, 0x2, 0x1a2, 0x1a3, 0x7, 0x77, 0x2, 0x2, 0x1a3, 0x1a4, 0x5, 0xd6, 
    0x6c, 0x2, 0x1a4, 0x1a5, 0x7, 0x77, 0x2, 0x2, 0x1a5, 0x1a6, 0x7, 0x3c, 
    0x2, 0x2, 0x1a6, 0x1a7, 0x7, 0x77, 0x2, 0x2, 0x1a7, 0x1a8, 0x5, 0xd6, 
    0x6c, 0x2, 0x1a8, 0x1d, 0x3, 0x2, 0x2, 0x2, 0x1a9, 0x1b4, 0x5, 0x20, 
    0x11, 0x2, 0x1aa, 0x1ac, 0x7, 0x77, 0x2, 0x2, 0x1ab, 0x1aa, 0x3, 0x2, 
    0x2, 0x2, 0x1ab, 0x1ac, 0x3, 0x2, 0x2, 0x2, 0x1ac, 0x1ad, 0x3, 0x2, 
    0x2, 0x2, 0x1ad, 0x1af, 0x7, 0x6, 0x2, 0x2, 0x1ae, 0x1b0, 0x7, 0x77, 
    0x2, 0x2, 0x1af, 0x1ae, 0x3, 0x2, 0x2, 0x2, 0x1af, 0x1b0, 0x3, 0x2, 
    0x2, 0x2, 0x1b0, 0x1b1, 0x3, 0x2, 0x2, 0x2, 0x1b1, 0x1b3, 0x5, 0x20, 
    0x11, 0x2, 0x1b2, 0x1ab, 0x3, 0x2, 0x2, 0x2, 0x1b3, 0x1b6, 0x3, 0x2, 
    0x2, 0x2, 0x1b4, 0x1b2, 0x3, 0x2, 0x2, 0x2, 0x1b4, 0x1b5, 0x3, 0x2, 
    0x2, 0x2, 0x1b5, 0x1f, 0x3, 0x2, 0x2, 0x2, 0x1b6, 0x1b4, 0x3, 0x2, 0x2, 
    0x2, 0x1b7, 0x1b8, 0x7, 0x31, 0x2, 0x2, 0x1b8, 0x1b9, 0x7, 0x77, 0x2, 
    0x2, 0x1b9, 0x1ba, 0x5, 0x22, 0x12, 0x2, 0x1ba, 0x1bb, 0x7, 0x77, 0x2, 
    0x2, 0x1bb, 0x1bc, 0x7, 0x3c, 0x2, 0x2, 0x1bc, 0x1bd, 0x7, 0x77, 0x2, 
    0x2, 0x1bd, 0x1be, 0x5, 0x22, 0x12, 0x2, 0x1be, 0x21, 0x3, 0x2, 0x2, 
    0x2, 0x1bf, 0x1ca, 0x5, 0xdc, 0x6f, 0x2, 0x1c0, 0x1c2, 0x7, 0x77, 0x2, 
    0x2, 0x1c1, 0x1c0, 0x3, 0x2, 0x2, 0x2, 0x1c1, 0x1c2, 0x3, 0x2, 0x2, 
    0x2, 0x1c2, 0x1c3, 0x3, 0x2, 0x2, 0x2, 0x1c3, 0x1c5, 0x7, 0x8, 0x2, 
    0x2, 0x1c4, 0x1c6, 0x7, 0x77, 0x2, 0x2, 0x1c5, 0x1c4, 0x3, 0x2, 0x2, 
    0x2, 0x1c5, 0x1c6, 0x3, 0x2, 0x2, 0x2, 0x1c6, 0x1c7, 0x3, 0x2, 0x2, 
    0x2, 0x1c7, 0x1c9, 0x5, 0xdc, 0x6f, 0x2, 0x1c8, 0x1c1, 0x3, 0x2, 0x2, 
    0x2, 0x1c9, 0x1cc, 0x3, 0x2, 0x2, 0x2, 0x1ca, 0x1c8, 0x3, 0x2, 0x2, 
    0x2, 0x1ca, 0x1cb, 0x3, 0x2, 0x2, 0x2, 0x1cb, 0x23, 0x3, 0x2, 0x2, 0x2, 
    0x1cc, 0x1ca, 0x3, 0x2, 0x2, 0x2, 0x1cd, 0x1d8, 0x5, 0x26, 0x14, 0x2, 
    0x1ce, 0x1d0, 0x7, 0x77, 0x2, 0x2, 0x1cf, 0x1ce, 0x3, 0x2, 0x2, 0x2, 
    0x1cf, 0x1d0, 0x3, 0x2, 0x2, 0x2, 0x1d0, 0x1d1, 0x3, 0x2, 0x2, 0x2, 
    0x1d1, 0x1d3, 0x7, 0x6, 0x2, 0x2, 0x1d2, 0x1d4, 0x7, 0x77, 0x2, 0x2, 
    0x1d3, 0x1d2, 0x3, 0x2, 0x2, 0x2, 0x1d3, 0x1d4, 0x3, 0x2, 0x2, 0x2, 
    0x1d4, 0x1d5, 0x3, 0x2, 0x2, 0x2, 0x1d5, 0x1d7, 0x5, 0x26, 0x14, 0x2, 
    0x1d6, 0x1cf, 0x3, 0x2, 0x2, 0x2, 0x1d7, 0x1da, 0x3, 0x2, 0x2, 0x2, 
    0x1d8, 0x1d6, 0x3, 0x2, 0x2, 0x2, 0x1d8, 0x1d9, 0x3, 0x2, 0x2, 0x2, 
    0x1d9, 0x25, 0x3, 0x2, 0x2, 0x2, 0x1da, 0x1d8, 0x3, 0x2, 0x2, 0x2, 0x1db, 
    0x1dc, 0x5, 0xd6, 0x6c, 0x2, 0x1dc, 0x1dd, 0x7, 0x77, 0x2, 0x2, 0x1dd, 
    0x1de, 0x5, 0x2a, 0x16, 0x2, 0x1de, 0x27, 0x3, 0x2, 0x2, 0x2, 0x1df, 
    0x1e0, 0x7, 0x39, 0x2, 0x2, 0x1e0, 0x1e1, 0x7, 0x77, 0x2, 0x2, 0x1e1, 
    0x1e3, 0x7, 0x3a, 0x2, 0x2, 0x1e2, 0x1e4, 0x7, 0x77, 0x2, 0x2, 0x1e3, 
    0x1e2, 0x3, 0x2, 0x2, 0x2, 0x1e3, 0x1e4, 0x3, 0x2, 0x2, 0x2, 0x1e4, 
    0x1e5, 0x3, 0x2, 0x2, 0x2, 0x1e5, 0x1e7, 0x7, 0x4, 0x2, 0x2, 0x1e6, 
    0x1e8, 0x7, 0x77, 0x2, 0x2, 0x1e7, 0x1e6, 0x3, 0x2, 0x2, 0x2, 0x1e7, 
    0x1e8, 0x3, 0x2, 0x2, 0x2, 0x1e8, 0x1e9, 0x3, 0x2, 0x2, 0x2, 0x1e9, 
    0x1eb, 0x5, 0xd6, 0x6c, 0x2, 0x1ea, 0x1ec, 0x7, 0x77, 0x2, 0x2, 0x1eb, 
    0x1ea, 0x3, 0x2, 0x2, 0x2, 0x1eb, 0x1ec, 0x3, 0x2, 0x2, 0x2, 0x1ec, 
    0x1ed, 0x3, 0x2, 0x2, 0x2, 0x1ed, 0x1ee, 0x7, 0x5, 0x2, 0x2, 0x1ee, 
    0x29, 0x3, 0x2, 0x2, 0x2, 0x1ef, 0x1f4, 0x5, 0xde, 0x70, 0x2, 0x1f0, 
    0x1f1, 0x5, 0xde, 0x70, 0x2, 0x1f1, 0x1f2, 0x5, 0x2c, 0x17, 0x2, 0x1f2, 
    0x1f4, 0x3, 0x2, 0x2, 0x2, 0x1f3, 0x1ef, 0x3, 0x2, 0x2, 0x2, 0x1f3, 
    0x1f0, 0x3, 0x2, 0x2, 0x2, 0x1f4, 0x2b, 0x3, 0x2, 0x2, 0x2, 0x1f5, 0x1f9, 
    0x5, 0x2e, 0x18, 0x2, 0x1f6, 0x1f8, 0x5, 0x2e, 0x18, 0x2, 0x1f7, 0x1f6, 
    0x3, 0x2, 0x2, 0x2, 0x1f8, 0x1fb, 0x3, 0x2, 0x2, 0x2, 0x1f9, 0x1f7, 
    0x3, 0x2, 0x2, 0x2, 0x1f9, 0x1fa, 0x3, 0x2, 0x2, 0x2, 0x1fa, 0x2d, 0x3, 
    0x2, 0x2, 0x2, 0x1fb, 0x1f9, 0x3, 0x2, 0x2, 0x2, 0x1fc, 0x1fd, 0x7, 
    0x9, 0x2, 0x2, 0x1fd, 0x1fe, 0x7, 0xa, 0x2, 0x2, 0x1fe, 0x2f, 0x3, 0x2, 
    0x2, 0x2, 0x1ff, 0x202, 0x5, 0x32, 0x1a, 0x2, 0x200, 0x202, 0x5, 0x34, 
    0x1b, 0x2, 0x201, 0x1ff, 0x3, 0x2, 0x2, 0x2, 0x201, 0x200, 0x3, 0x2, 
    0x2, 0x2, 0x202, 0x31, 0x3, 0x2, 0x2, 0x2, 0x203, 0x204, 0x7, 0x3d, 
    0x2, 0x2, 0x204, 0x33, 0x3, 0x2, 0x2, 0x2, 0x205, 0x206, 0x7, 0x3e, 
    0x2, 0x2, 0x206, 0x35, 0x3, 0x2, 0x2, 0x2, 0x207, 0x208, 0x5, 0x38, 
    0x1d, 0x2, 0x208, 0x37, 0x3, 0x2, 0x2, 0x2, 0x209, 0x20a, 0x5, 0x3a, 
    0x1e, 0x2, 0x20a, 0x39, 0x3, 0x2, 0x2, 0x2, 0x20b, 0x212, 0x5, 0x3e, 
    0x20, 0x2, 0x20c, 0x20e, 0x7, 0x77, 0x2, 0x2, 0x20d, 0x20c, 0x3, 0x2, 
    0x2, 0x2, 0x20d, 0x20e, 0x3, 0x2, 0x2, 0x2, 0x20e, 0x20f, 0x3, 0x2, 
    0x2, 0x2, 0x20f, 0x211, 0x5, 0x3c, 0x1f, 0x2, 0x210, 0x20d, 0x3, 0x2, 
    0x2, 0x2, 0x211, 0x214, 0x3, 0x2, 0x2, 0x2, 0x212, 0x210, 0x3, 0x2, 
    0x2, 0x2, 0x212, 0x213, 0x3, 0x2, 0x2, 0x2, 0x213, 0x221, 0x3, 0x2, 
    0x2, 0x2, 0x214, 0x212, 0x3, 0x2, 0x2, 0x2, 0x215, 0x217, 0x5, 0x58, 
    0x2d, 0x2, 0x216, 0x218, 0x7, 0x77, 0x2, 0x2, 0x217, 0x216, 0x3, 0x2, 
    0x2, 0x2, 0x217, 0x218, 0x3, 0x2, 0x2, 0x2, 0x218, 0x21a, 0x3, 0x2, 
    0x2, 0x2, 0x219, 0x215, 0x3, 0x2, 0x2, 0x2, 0x21a, 0x21b, 0x3, 0x2, 
    0x2, 0x2, 0x21b, 0x219, 0x3, 0x2, 0x2, 0x2, 0x21b, 0x21c, 0x3, 0x2, 
    0x2, 0x2, 0x21c, 0x21d, 0x3, 0x2, 0x2, 0x2, 0x21d, 0x21e, 0x5, 0x3e, 
    0x20, 0x2, 0x21e, 0x21f, 0x8, 0x1e, 0x1, 0x2, 0x21f, 0x221, 0x3, 0x2, 
    0x2, 0x2, 0x220, 0x20b, 0x3, 0x2, 0x2, 0x2, 0x220, 0x219, 0x3, 0x2, 
    0x2, 0x2, 0x221, 0x3b, 0x3, 0x2, 0x2, 0x2, 0x222, 0x223, 0x7, 0x3f, 
    0x2, 0x2, 0x223, 0x224, 0x7, 0x77, 0x2, 0x2, 0x224, 0x226, 0x7, 0x40, 
    0x2, 0x2, 0x225, 0x227, 0x7, 0x77, 0x2, 0x2, 0x226, 0x225, 0x3, 0x2, 
    0x2, 0x2, 0x226, 0x227, 0x3, 0x2, 0x2, 0x2, 0x227, 0x228, 0x3, 0x2, 
    0x2, 0x2, 0x228, 0x22f, 0x5, 0x3e, 0x20, 0x2, 0x229, 0x22b, 0x7, 0x3f, 
    0x2, 0x2, 0x22a, 0x22c, 0x7, 0x77, 0x2, 0x2, 0x22b, 0x22a, 0x3, 0x2, 
    0x2, 0x2, 0x22b, 0x22c, 0x3, 0x2, 0x2, 0x2, 0x22c, 0x22d, 0x3, 0x2, 
    0x2, 0x2, 0x22d, 0x22f, 0x5, 0x3e, 0x20, 0x2, 0x22e, 0x222, 0x3, 0x2, 
    0x2, 0x2, 0x22e, 0x229, 0x3, 0x2, 0x2, 0x2, 0x22f, 0x3d, 0x3, 0x2, 0x2, 
    0x2, 0x230, 0x233, 0x5, 0x40, 0x21, 0x2, 0x231, 0x233, 0x5, 0x42, 0x22, 
    0x2, 0x232, 0x230, 0x3, 0x2, 0x2, 0x2, 0x232, 0x231, 0x3, 0x2, 0x2, 
    0x2, 0x233, 0x3f, 0x3, 0x2, 0x2, 0x2, 0x234, 0x236, 0x5, 0x48, 0x25, 
    0x2, 0x235, 0x237, 0x7, 0x77, 0x2, 0x2, 0x236, 0x235, 0x3, 0x2, 0x2, 
    0x2, 0x236, 0x237, 0x3, 0x2, 0x2, 0x2, 0x237, 0x239, 0x3, 0x2, 0x2, 
    0x2, 0x238, 0x234, 0x3, 0x2, 0x2, 0x2, 0x239, 0x23c, 0x3, 0x2, 0x2, 
    0x2, 0x23a, 0x238, 0x3, 0x2, 0x2, 0x2, 0x23a, 0x23b, 0x3, 0x2, 0x2, 
    0x2, 0x23b, 0x23d, 0x3, 0x2, 0x2, 0x2, 0x23c, 0x23a, 0x3, 0x2, 0x2, 
    0x2, 0x23d, 0x262, 0x5, 0x58, 0x2d, 0x2, 0x23e, 0x240, 0x5, 0x48, 0x25, 
    0x2, 0x23f, 0x241, 0x7, 0x77, 0x2, 0x2, 0x240, 0x23f, 0x3, 0x2, 0x2, 
    0x2, 0x240, 0x241, 0x3, 0x2, 0x2, 0x2, 0x241, 0x243, 0x3, 0x2, 0x2, 
    0x2, 0x242, 0x23e, 0x3, 0x2, 0x2, 0x2, 0x243, 0x246, 0x3, 0x2, 0x2, 
    0x2, 0x244, 0x242, 0x3, 0x2, 0x2, 0x2, 0x244, 0x245, 0x3, 0x2, 0x2, 
    0x2, 0x245, 0x247, 0x3, 0x2, 0x2, 0x2, 0x246, 0x244, 0x3, 0x2, 0x2, 
    0x2, 0x247, 0x24e, 0x5, 0x46, 0x24, 0x2, 0x248, 0x24a, 0x7, 0x77, 0x2, 
    0x2, 0x249, 0x248, 0x3, 0x2, 0x2, 0x2, 0x249, 0x24a, 0x3, 0x2, 0x2, 
    0x2, 0x24a, 0x24b, 0x3, 0x2, 0x2, 0x2, 0x24b, 0x24d, 0x5, 0x46, 0x24, 
    0x2, 0x24c, 0x249, 0x3, 0x2, 0x2, 0x2, 0x24d, 0x250, 0x3, 0x2, 0x2, 
    0x2, 0x24e, 0x24c, 0x3, 0x2, 0x2, 0x2, 0x24e, 0x24f, 0x3, 0x2, 0x2, 
    0x2, 0x24f, 0x255, 0x3, 0x2, 0x2, 0x2, 0x250, 0x24e, 0x3, 0x2, 0x2, 
    0x2, 0x251, 0x253, 0x7, 0x77, 0x2, 0x2, 0x252, 0x251, 0x3, 0x2, 0x2, 
    0x2, 0x252, 0x253, 0x3, 0x2, 0x2, 0x2, 0x253, 0x254, 0x3, 0x2, 0x2, 
    0x2, 0x254, 0x256, 0x5, 0x58, 0x2d, 0x2, 0x255, 0x252, 0x3, 0x2, 0x2, 
    0x2, 0x255, 0x256, 0x3, 0x2, 0x2, 0x2, 0x256, 0x262, 0x3, 0x2, 0x2, 
    0x2, 0x257, 0x259, 0x5, 0x48, 0x25, 0x2, 0x258, 0x25a, 0x7, 0x77, 0x2, 
    0x2, 0x259, 0x258, 0x3, 0x2, 0x2, 0x2, 0x259, 0x25a, 0x3, 0x2, 0x2, 
    0x2, 0x25a, 0x25c, 0x3, 0x2, 0x2, 0x2, 0x25b, 0x257, 0x3, 0x2, 0x2, 
    0x2, 0x25c, 0x25f, 0x3, 0x2, 0x2, 0x2, 0x25d, 0x25b, 0x3, 0x2, 0x2, 
    0x2, 0x25d, 0x25e, 0x3, 0x2, 0x2, 0x2, 0x25e, 0x260, 0x3, 0x2, 0x2, 
    0x2, 0x25f, 0x25d, 0x3, 0x2, 0x2, 0x2, 0x260, 0x262, 0x8, 0x21, 0x1, 
    0x2, 0x261, 0x23a, 0x3, 0x2, 0x2, 0x2, 0x261, 0x244, 0x3, 0x2, 0x2, 
    0x2, 0x261, 0x25d, 0x3, 0x2, 0x2, 0x2, 0x262, 0x41, 0x3, 0x2, 0x2, 0x2, 
    0x263, 0x265, 0x5, 0x44, 0x23, 0x2, 0x264, 0x266, 0x7, 0x77, 0x2, 0x2, 
    0x265, 0x264, 0x3, 0x2, 0x2, 0x2, 0x265, 0x266, 0x3, 0x2, 0x2, 0x2, 
    0x266, 0x268, 0x3, 0x2, 0x2, 0x2, 0x267, 0x263, 0x3, 0x2, 0x2, 0x2, 
    0x268, 0x269, 0x3, 0x2, 0x2, 0x2, 0x269, 0x267, 0x3, 0x2, 0x2, 0x2, 
    0x269, 0x26a, 0x3, 0x2, 0x2, 0x2, 0x26a, 0x26b, 0x3, 0x2, 0x2, 0x2, 
    0x26b, 0x26c, 0x5, 0x40, 0x21, 0x2, 0x26c, 0x43, 0x3, 0x2, 0x2, 0x2, 
    0x26d, 0x26f, 0x5, 0x48, 0x25, 0x2, 0x26e, 0x270, 0x7, 0x77, 0x2, 0x2, 
    0x26f, 0x26e, 0x3, 0x2, 0x2, 0x2, 0x26f, 0x270, 0x3, 0x2, 0x2, 0x2, 
    0x270, 0x272, 0x3, 0x2, 0x2, 0x2, 0x271, 0x26d, 0x3, 0x2, 0x2, 0x2, 
    0x272, 0x275, 0x3, 0x2, 0x2, 0x2, 0x273, 0x271, 0x3, 0x2, 0x2, 0x2, 
    0x273, 0x274, 0x3, 0x2, 0x2, 0x2, 0x274, 0x27c, 0x3, 0x2, 0x2, 0x2, 
    0x275, 0x273, 0x3, 0x2, 0x2, 0x2, 0x276, 0x278, 0x5, 0x46, 0x24, 0x2, 
    0x277, 0x279, 0x7, 0x77, 0x2, 0x2, 0x278, 0x277, 0x3, 0x2, 0x2, 0x2, 
    0x278, 0x279, 0x3, 0x2, 0x2, 0x2, 0x279, 0x27b, 0x3, 0x2, 0x2, 0x2, 
    0x27a, 0x276, 0x3, 0x2, 0x2, 0x2, 0x27b, 0x27e, 0x3, 0x2, 0x2, 0x2, 
    0x27c, 0x27a, 0x3, 0x2, 0x2, 0x2, 0x27c, 0x27d, 0x3, 0x2, 0x2, 0x2, 
    0x27d, 0x27f, 0x3, 0x2, 0x2, 0x2, 0x27e, 0x27c, 0x3, 0x2, 0x2, 0x2, 
    0x27f, 0x280, 0x5, 0x56, 0x2c, 0x2, 0x280, 0x45, 0x3, 0x2, 0x2, 0x2, 
    0x281, 0x285, 0x5, 0x4e, 0x28, 0x2, 0x282, 0x285, 0x5, 0x50, 0x29, 0x2, 
    0x283, 0x285, 0x5, 0x54, 0x2b, 0x2, 0x284, 0x281, 0x3, 0x2, 0x2, 0x2, 
    0x284, 0x282, 0x3, 0x2, 0x2, 0x2, 0x284, 0x283, 0x3, 0x2, 0x2, 0x2, 
    0x285, 0x47, 0x3, 0x2, 0x2, 0x2, 0x286, 0x289, 0x5, 0x4a, 0x26, 0x2, 
    0x287, 0x289, 0x5, 0x4c, 0x27, 0x2, 0x288, 0x286, 0x3, 0x2, 0x2, 0x2, 
    0x288, 0x287, 0x3, 0x2, 0x2, 0x2, 0x289, 0x49, 0x3, 0x2, 0x2, 0x2, 0x28a, 
    0x28b, 0x7, 0x41, 0x2, 0x2, 0x28b, 0x28d, 0x7, 0x77, 0x2, 0x2, 0x28c, 
    0x28a, 0x3, 0x2, 0x2, 0x2, 0x28c, 0x28d, 0x3, 0x2, 0x2, 0x2, 0x28d, 
    0x28e, 0x3, 0x2, 0x2, 0x2, 0x28e, 0x290, 0x7, 0x42, 0x2, 0x2, 0x28f, 
    0x291, 0x7, 0x77, 0x2, 0x2, 0x290, 0x28f, 0x3, 0x2, 0x2, 0x2, 0x290, 
    0x291, 0x3, 0x2, 0x2, 0x2, 0x291, 0x292, 0x3, 0x2, 0x2, 0x2, 0x292, 
    0x297, 0x5, 0x6a, 0x36, 0x2, 0x293, 0x295, 0x7, 0x77, 0x2, 0x2, 0x294, 
    0x293, 0x3, 0x2, 0x2, 0x2, 0x294, 0x295, 0x3, 0x2, 0x2, 0x2, 0x295, 
    0x296, 0x3, 0x2, 0x2, 0x2, 0x296, 0x298, 0x5, 0x68, 0x35, 0x2, 0x297, 
    0x294, 0x3, 0x2, 0x2, 0x2, 0x297, 0x298, 0x3, 0x2, 0x2, 0x2, 0x298, 
    0x4b, 0x3, 0x2, 0x2, 0x2, 0x299, 0x29b, 0x7, 0x43, 0x2, 0x2, 0x29a, 
    0x29c, 0x7, 0x77, 0x2, 0x2, 0x29b, 0x29a, 0x3, 0x2, 0x2, 0x2, 0x29b, 
    0x29c, 0x3, 0x2, 0x2, 0x2, 0x29c, 0x29d, 0x3, 0x2, 0x2, 0x2, 0x29d, 
    0x29e, 0x5, 0x88, 0x45, 0x2, 0x29e, 0x29f, 0x7, 0x77, 0x2, 0x2, 0x29f, 
    0x2a0, 0x7, 0x4b, 0x2, 0x2, 0x2a0, 0x2a1, 0x7, 0x77, 0x2, 0x2, 0x2a1, 
    0x2a2, 0x5, 0xce, 0x68, 0x2, 0x2a2, 0x4d, 0x3, 0x2, 0x2, 0x2, 0x2a3, 
    0x2a5, 0x7, 0x44, 0x2, 0x2, 0x2a4, 0x2a6, 0x7, 0x77, 0x2, 0x2, 0x2a5, 
    0x2a4, 0x3, 0x2, 0x2, 0x2, 0x2a5, 0x2a6, 0x3, 0x2, 0x2, 0x2, 0x2a6, 
    0x2a7, 0x3, 0x2, 0x2, 0x2, 0x2a7, 0x2a8, 0x5, 0x6a, 0x36, 0x2, 0x2a8, 
    0x4f, 0x3, 0x2, 0x2, 0x2, 0x2a9, 0x2ab, 0x7, 0x45, 0x2, 0x2, 0x2aa, 
    0x2ac, 0x7, 0x77, 0x2, 0x2, 0x2ab, 0x2aa, 0x3, 0x2, 0x2, 0x2, 0x2ab, 
    0x2ac, 0x3, 0x2, 0x2, 0x2, 0x2ac, 0x2ad, 0x3, 0x2, 0x2, 0x2, 0x2ad, 
    0x2b8, 0x5, 0x52, 0x2a, 0x2, 0x2ae, 0x2b0, 0x7, 0x77, 0x2, 0x2, 0x2af, 
    0x2ae, 0x3, 0x2, 0x2, 0x2, 0x2af, 0x2b0, 0x3, 0x2, 0x2, 0x2, 0x2b0, 
    0x2b1, 0x3, 0x2, 0x2, 0x2, 0x2b1, 0x2b3, 0x7, 0x6, 0x2, 0x2, 0x2b2, 
    0x2b4, 0x7, 0x77, 0x2, 0x2, 0x2b3, 0x2b2, 0x3, 0x2, 0x2, 0x2, 0x2b3, 
    0x2b4, 0x3, 0x2, 0x2, 0x2, 0x2b4, 0x2b5, 0x3, 0x2, 0x2, 0x2, 0x2b5, 
    0x2b7, 0x5, 0x52, 0x2a, 0x2, 0x2b6, 0x2af, 0x3, 0x2, 0x2, 0x2, 0x2b7, 
    0x2ba, 0x3, 0x2, 0x2, 0x2, 0x2b8, 0x2b6, 0x3, 0x2, 0x2, 0x2, 0x2b8, 
    0x2b9, 0x3, 0x2, 0x2, 0x2, 0x2b9, 0x51, 0x3, 0x2, 0x2, 0x2, 0x2ba, 0x2b8, 
    0x3, 0x2, 0x2, 0x2, 0x2bb, 0x2bd, 0x5, 0xd4, 0x6b, 0x2, 0x2bc, 0x2be, 
    0x7, 0x77, 0x2, 0x2, 0x2bd, 0x2bc, 0x3, 0x2, 0x2, 0x2, 0x2bd, 0x2be, 
    0x3, 0x2, 0x2, 0x2, 0x2be, 0x2bf, 0x3, 0x2, 0x2, 0x2, 0x2bf, 0x2c1, 
    0x7, 0x7, 0x2, 0x2, 0x2c0, 0x2c2, 0x7, 0x77, 0x2, 0x2, 0x2c1, 0x2c0, 
    0x3, 0x2, 0x2, 0x2, 0x2c1, 0x2c2, 0x3, 0x2, 0x2, 0x2, 0x2c2, 0x2c3, 
    0x3, 0x2, 0x2, 0x2, 0x2c3, 0x2c4, 0x5, 0x88, 0x45, 0x2, 0x2c4, 0x53, 
    0x3, 0x2, 0x2, 0x2, 0x2c5, 0x2c7, 0x7, 0x46, 0x2, 0x2, 0x2c6, 0x2c8, 
    0x7, 0x77, 0x2, 0x2, 0x2c7, 0x2c6, 0x3, 0x2, 0x2, 0x2, 0x2c7, 0x2c8, 
    0x3, 0x2, 0x2, 0x2, 0x2c8, 0x2c9, 0x3, 0x2, 0x2, 0x2, 0x2c9, 0x2d4, 
    0x5, 0x88, 0x45, 0x2, 0x2ca, 0x2cc, 0x7, 0x77, 0x2, 0x2, 0x2cb, 0x2ca, 
    0x3, 0x2, 0x2, 0x2, 0x2cb, 0x2cc, 0x3, 0x2, 0x2, 0x2, 0x2cc, 0x2cd, 
    0x3, 0x2, 0x2, 0x2, 0x2cd, 0x2cf, 0x7, 0x6, 0x2, 0x2, 0x2ce, 0x2d0, 
    0x7, 0x77, 0x2, 0x2, 0x2cf, 0x2ce, 0x3, 0x2, 0x2, 0x2, 0x2cf, 0x2d0, 
    0x3, 0x2, 0x2, 0x2, 0x2d0, 0x2d1, 0x3, 0x2, 0x2, 0x2, 0x2d1, 0x2d3, 
    0x5, 0x88, 0x45, 0x2, 0x2d2, 0x2cb, 0x3, 0x2, 0x2, 0x2, 0x2d3, 0x2d6, 
    0x3, 0x2, 0x2, 0x2, 0x2d4, 0x2d2, 0x3, 0x2, 0x2, 0x2, 0x2d4, 0x2d5, 
    0x3, 0x2, 0x2, 0x2, 0x2d5, 0x55, 0x3, 0x2, 0x2, 0x2, 0x2d6, 0x2d4, 0x3, 
    0x2, 0x2, 0x2, 0x2d7, 0x2d8, 0x7, 0x47, 0x2, 0x2, 0x2d8, 0x2dd, 0x5, 
    0x5a, 0x2e, 0x2, 0x2d9, 0x2db, 0x7, 0x77, 0x2, 0x2, 0x2da, 0x2d9, 0x3, 
    0x2, 0x2, 0x2, 0x2da, 0x2db, 0x3, 0x2, 0x2, 0x2, 0x2db, 0x2dc, 0x3, 
    0x2, 0x2, 0x2, 0x2dc, 0x2de, 0x5, 0x68, 0x35, 0x2, 0x2dd, 0x2da, 0x3, 
    0x2, 0x2, 0x2, 0x2dd, 0x2de, 0x3, 0x2, 0x2, 0x2, 0x2de, 0x57, 0x3, 0x2, 
    0x2, 0x2, 0x2df, 0x2e0, 0x7, 0x48, 0x2, 0x2, 0x2e0, 0x2e1, 0x5, 0x5a, 
    0x2e, 0x2, 0x2e1, 0x59, 0x3, 0x2, 0x2, 0x2, 0x2e2, 0x2e4, 0x7, 0x77, 
    0x2, 0x2, 0x2e3, 0x2e2, 0x3, 0x2, 0x2, 0x2, 0x2e3, 0x2e4, 0x3, 0x2, 
    0x2, 0x2, 0x2e4, 0x2e5, 0x3, 0x2, 0x2, 0x2, 0x2e5, 0x2e7, 0x7, 0x49, 
    0x2, 0x2, 0x2e6, 0x2e3, 0x3, 0x2, 0x2, 0x2, 0x2e6, 0x2e7, 0x3, 0x2, 
    0x2, 0x2, 0x2e7, 0x2e8, 0x3, 0x2, 0x2, 0x2, 0x2e8, 0x2e9, 0x7, 0x77, 
    0x2, 0x2, 0x2e9, 0x2ec, 0x5, 0x5c, 0x2f, 0x2, 0x2ea, 0x2eb, 0x7, 0x77, 
    0x2, 0x2, 0x2eb, 0x2ed, 0x5, 0x60, 0x31, 0x2, 0x2ec, 0x2ea, 0x3, 0x2, 
    0x2, 0x2, 0x2ec, 0x2ed, 0x3, 0x2, 0x2, 0x2, 0x2ed, 0x2f0, 0x3, 0x2, 
    0x2, 0x2, 0x2ee, 0x2ef, 0x7, 0x77, 0x2, 0x2, 0x2ef, 0x2f1, 0x5, 0x62, 
    0x32, 0x2, 0x2f0, 0x2ee, 0x3, 0x2, 0x2, 0x2, 0x2f0, 0x2f1, 0x3, 0x2, 
    0x2, 0x2, 0x2f1, 0x2f4, 0x3, 0x2, 0x2, 0x2, 0x2f2, 0x2f3, 0x7, 0x77, 
    0x2, 0x2, 0x2f3, 0x2f5, 0x5, 0x64, 0x33, 0x2, 0x2f4, 0x2f2, 0x3, 0x2, 
    0x2, 0x2, 0x2f4, 0x2f5, 0x3, 0x2, 0x2, 0x2, 0x2f5, 0x5b, 0x3, 0x2, 0x2, 
    0x2, 0x2f6, 0x301, 0x7, 0x4a, 0x2, 0x2, 0x2f7, 0x2f9, 0x7, 0x77, 0x2, 
    0x2, 0x2f8, 0x2f7, 0x3, 0x2, 0x2, 0x2, 0x2f8, 0x2f9, 0x3, 0x2, 0x2, 
    0x2, 0x2f9, 0x2fa, 0x3, 0x2, 0x2, 0x2, 0x2fa, 0x2fc, 0x7, 0x6, 0x2, 
    0x2, 0x2fb, 0x2fd, 0x7, 0x77, 0x2, 0x2, 0x2fc, 0x2fb, 0x3, 0x2, 0x2, 
    0x2, 0x2fc, 0x2fd, 0x3, 0x2, 0x2, 0x2, 0x2fd, 0x2fe, 0x3, 0x2, 0x2, 
    0x2, 0x2fe, 0x300, 0x5, 0x5e, 0x30, 0x2, 0x2ff, 0x2f8, 0x3, 0x2, 0x2, 
    0x2, 0x300, 0x303, 0x3, 0x2, 0x2, 0x2, 0x301, 0x2ff, 0x3, 0x2, 0x2, 
    0x2, 0x301, 0x302, 0x3, 0x2, 0x2, 0x2, 0x302, 0x313, 0x3, 0x2, 0x2, 
    0x2, 0x303, 0x301, 0x3, 0x2, 0x2, 0x2, 0x304, 0x30f, 0x5, 0x5e, 0x30, 
    0x2, 0x305, 0x307, 0x7, 0x77, 0x2, 0x2, 0x306, 0x305, 0x3, 0x2, 0x2, 
    0x2, 0x306, 0x307, 0x3, 0x2, 0x2, 0x2, 0x307, 0x308, 0x3, 0x2, 0x2, 
    0x2, 0x308, 0x30a, 0x7, 0x6, 0x2, 0x2, 0x309, 0x30b, 0x7, 0x77, 0x2, 
    0x2, 0x30a, 0x309, 0x3, 0x2, 0x2, 0x2, 0x30a, 0x30b, 0x3, 0x2, 0x2, 
    0x2, 0x30b, 0x30c, 0x3, 0x2, 0x2, 0x2, 0x30c, 0x30e, 0x5, 0x5e, 0x30, 
    0x2, 0x30d, 0x306, 0x3, 0x2, 0x2, 0x2, 0x30e, 0x311, 0x3, 0x2, 0x2, 
    0x2, 0x30f, 0x30d, 0x3, 0x2, 0x2, 0x2, 0x30f, 0x310, 0x3, 0x2, 0x2, 
    0x2, 0x310, 0x313, 0x3, 0x2, 0x2, 0x2, 0x311, 0x30f, 0x3, 0x2, 0x2, 
    0x2, 0x312, 0x2f6, 0x3, 0x2, 0x2, 0x2, 0x312, 0x304, 0x3, 0x2, 0x2, 
    0x2, 0x313, 0x5d, 0x3, 0x2, 0x2, 0x2, 0x314, 0x315, 0x5, 0x88, 0x45, 
    0x2, 0x315, 0x316, 0x7, 0x77, 0x2, 0x2, 0x316, 0x317, 0x7, 0x4b, 0x2, 
    0x2, 0x317, 0x318, 0x7, 0x77, 0x2, 0x2, 0x318, 0x319, 0x5, 0xce, 0x68, 
    0x2, 0x319, 0x31c, 0x3, 0x2, 0x2, 0x2, 0x31a, 0x31c, 0x5, 0x88, 0x45, 
    0x2, 0x31b, 0x314, 0x3, 0x2, 0x2, 0x2, 0x31b, 0x31a, 0x3, 0x2, 0x2, 
    0x2, 0x31c, 0x5f, 0x3, 0x2, 0x2, 0x2, 0x31d, 0x31e, 0x7, 0x4c, 0x2, 
    0x2, 0x31e, 0x31f, 0x7, 0x77, 0x2, 0x2, 0x31f, 0x320, 0x7, 0x4d, 0x2, 
    0x2, 0x320, 0x321, 0x7, 0x77, 0x2, 0x2, 0x321, 0x329, 0x5, 0x66, 0x34, 
    0x2, 0x322, 0x324, 0x7, 0x6, 0x2, 0x2, 0x323, 0x325, 0x7, 0x77, 0x2, 
    0x2, 0x324, 0x323, 0x3, 0x2, 0x2, 0x2, 0x324, 0x325, 0x3, 0x2, 0x2, 
    0x2, 0x325, 0x326, 0x3, 0x2, 0x2, 0x2, 0x326, 0x328, 0x5, 0x66, 0x34, 
    0x2, 0x327, 0x322, 0x3, 0x2, 0x2, 0x2, 0x328, 0x32b, 0x3, 0x2, 0x2, 
    0x2, 0x329, 0x327, 0x3, 0x2, 0x2, 0x2, 0x329, 0x32a, 0x3, 0x2, 0x2, 
    0x2, 0x32a, 0x61, 0x3, 0x2, 0x2, 0x2, 0x32b, 0x329, 0x3, 0x2, 0x2, 0x2, 
    0x32c, 0x32d, 0x7, 0x4e, 0x2, 0x2, 0x32d, 0x32e, 0x7, 0x77, 0x2, 0x2, 
    0x32e, 0x32f, 0x5, 0x88, 0x45, 0x2, 0x32f, 0x63, 0x3, 0x2, 0x2, 0x2, 
    0x330, 0x331, 0x7, 0x4f, 0x2, 0x2, 0x331, 0x332, 0x7, 0x77, 0x2, 0x2, 
    0x332, 0x333, 0x5, 0x88, 0x45, 0x2, 0x333, 0x65, 0x3, 0x2, 0x2, 0x2, 
    0x334, 0x339, 0x5, 0x88, 0x45, 0x2, 0x335, 0x337, 0x7, 0x77, 0x2, 0x2, 
    0x336, 0x335, 0x3, 0x2, 0x2, 0x2, 0x336, 0x337, 0x3, 0x2, 0x2, 0x2, 
    0x337, 0x338, 0x3, 0x2, 0x2, 0x2, 0x338, 0x33a, 0x9, 0x2, 0x2, 0x2, 
    0x339, 0x336, 0x3, 0x2, 0x2, 0x2, 0x339, 0x33a, 0x3, 0x2, 0x2, 0x2, 
    0x33a, 0x67, 0x3, 0x2, 0x2, 0x2, 0x33b, 0x33c, 0x7, 0x54, 0x2, 0x2, 
    0x33c, 0x33d, 0x7, 0x77, 0x2, 0x2, 0x33d, 0x33e, 0x5, 0x88, 0x45, 0x2, 
    0x33e, 0x69, 0x3, 0x2, 0x2, 0x2, 0x33f, 0x34a, 0x5, 0x6c, 0x37, 0x2, 
    0x340, 0x342, 0x7, 0x77, 0x2, 0x2, 0x341, 0x340, 0x3, 0x2, 0x2, 0x2, 
    0x341, 0x342, 0x3, 0x2, 0x2, 0x2, 0x342, 0x343, 0x3, 0x2, 0x2, 0x2, 
    0x343, 0x345, 0x7, 0x6, 0x2, 0x2, 0x344, 0x346, 0x7, 0x77, 0x2, 0x2, 
    0x345, 0x344, 0x3, 0x2, 0x2, 0x2, 0x345, 0x346, 0x3, 0x2, 0x2, 0x2, 
    0x346, 0x347, 0x3, 0x2, 0x2, 0x2, 0x347, 0x349, 0x5, 0x6c, 0x37, 0x2, 
    0x348, 0x341, 0x3, 0x2, 0x2, 0x2, 0x349, 0x34c, 0x3, 0x2, 0x2, 0x2, 
    0x34a, 0x348, 0x3, 0x2, 0x2, 0x2, 0x34a, 0x34b, 0x3, 0x2, 0x2, 0x2, 
    0x34b, 0x6b, 0x3, 0x2, 0x2, 0x2, 0x34c, 0x34a, 0x3, 0x2, 0x2, 0x2, 0x34d, 
    0x34e, 0x5, 0x6e, 0x38, 0x2, 0x34e, 0x6d, 0x3, 0x2, 0x2, 0x2, 0x34f, 
    0x350, 0x5, 0x70, 0x39, 0x2, 0x350, 0x6f, 0x3, 0x2, 0x2, 0x2, 0x351, 
    0x358, 0x5, 0x72, 0x3a, 0x2, 0x352, 0x354, 0x7, 0x77, 0x2, 0x2, 0x353, 
    0x352, 0x3, 0x2, 0x2, 0x2, 0x353, 0x354, 0x3, 0x2, 0x2, 0x2, 0x354, 
    0x355, 0x3, 0x2, 0x2, 0x2, 0x355, 0x357, 0x5, 0x74, 0x3b, 0x2, 0x356, 
    0x353, 0x3, 0x2, 0x2, 0x2, 0x357, 0x35a, 0x3, 0x2, 0x2, 0x2, 0x358, 
    0x356, 0x3, 0x2, 0x2, 0x2, 0x358, 0x359, 0x3, 0x2, 0x2, 0x2, 0x359, 
    0x360, 0x3, 0x2, 0x2, 0x2, 0x35a, 0x358, 0x3, 0x2, 0x2, 0x2, 0x35b, 
    0x35c, 0x7, 0x4, 0x2, 0x2, 0x35c, 0x35d, 0x5, 0x70, 0x39, 0x2, 0x35d, 
    0x35e, 0x7, 0x5, 0x2, 0x2, 0x35e, 0x360, 0x3, 0x2, 0x2, 0x2, 0x35f, 
    0x351, 0x3, 0x2, 0x2, 0x2, 0x35f, 0x35b, 0x3, 0x2, 0x2, 0x2, 0x360, 
    0x71, 0x3, 0x2, 0x2, 0x2, 0x361, 0x363, 0x7, 0x4, 0x2, 0x2, 0x362, 0x364, 
    0x7, 0x77, 0x2, 0x2, 0x363, 0x362, 0x3, 0x2, 0x2, 0x2, 0x363, 0x364, 
    0x3, 0x2, 0x2, 0x2, 0x364, 0x369, 0x3, 0x2, 0x2, 0x2, 0x365, 0x367, 
    0x5, 0xce, 0x68, 0x2, 0x366, 0x368, 0x7, 0x77, 0x2, 0x2, 0x367, 0x366, 
    0x3, 0x2, 0x2, 0x2, 0x367, 0x368, 0x3, 0x2, 0x2, 0x2, 0x368, 0x36a, 
    0x3, 0x2, 0x2, 0x2, 0x369, 0x365, 0x3, 0x2, 0x2, 0x2, 0x369, 0x36a, 
    0x3, 0x2, 0x2, 0x2, 0x36a, 0x36f, 0x3, 0x2, 0x2, 0x2, 0x36b, 0x36d, 
    0x5, 0x7e, 0x40, 0x2, 0x36c, 0x36e, 0x7, 0x77, 0x2, 0x2, 0x36d, 0x36c, 
    0x3, 0x2, 0x2, 0x2, 0x36d, 0x36e, 0x3, 0x2, 0x2, 0x2, 0x36e, 0x370, 
    0x3, 0x2, 0x2, 0x2, 0x36f, 0x36b, 0x3, 0x2, 0x2, 0x2, 0x36f, 0x370, 
    0x3, 0x2, 0x2, 0x2, 0x370, 0x375, 0x3, 0x2, 0x2, 0x2, 0x371, 0x373, 
    0x5, 0x7a, 0x3e, 0x2, 0x372, 0x374, 0x7, 0x77, 0x2, 0x2, 0x373, 0x372, 
    0x3, 0x2, 0x2, 0x2, 0x373, 0x374, 0x3, 0x2, 0x2, 0x2, 0x374, 0x376, 
    0x3, 0x2, 0x2, 0x2, 0x375, 0x371, 0x3, 0x2, 0x2, 0x2, 0x375, 0x376, 
    0x3, 0x2, 0x2, 0x2, 0x376, 0x377, 0x3, 0x2, 0x2, 0x2, 0x377, 0x38f, 
    0x7, 0x5, 0x2, 0x2, 0x378, 0x37a, 0x7, 0x77, 0x2, 0x2, 0x379, 0x378, 
    0x3, 0x2, 0x2, 0x2, 0x379, 0x37a, 0x3, 0x2, 0x2, 0x2, 0x37a, 0x37f, 
    0x3, 0x2, 0x2, 0x2, 0x37b, 0x37d, 0x5, 0xce, 0x68, 0x2, 0x37c, 0x37e, 
    0x7, 0x77, 0x2, 0x2, 0x37d, 0x37c, 0x3, 0x2, 0x2, 0x2, 0x37d, 0x37e, 
    0x3, 0x2, 0x2, 0x2, 0x37e, 0x380, 0x3, 0x2, 0x2, 0x2, 0x37f, 0x37b, 
    0x3, 0x2, 0x2, 0x2, 0x37f, 0x380, 0x3, 0x2, 0x2, 0x2, 0x380, 0x385, 
    0x3, 0x2, 0x2, 0x2, 0x381, 0x383, 0x5, 0x7e, 0x40, 0x2, 0x382, 0x384, 
    0x7, 0x77, 0x2, 0x2, 0x383, 0x382, 0x3, 0x2, 0x2, 0x2, 0x383, 0x384, 
    0x3, 0x2, 0x2, 0x2, 0x384, 0x386, 0x3, 0x2, 0x2, 0x2, 0x385, 0x381, 
    0x3, 0x2, 0x2, 0x2, 0x385, 0x386, 0x3, 0x2, 0x2, 0x2, 0x386, 0x38b, 
    0x3, 0x2, 0x2, 0x2, 0x387, 0x389, 0x5, 0x7a, 0x3e, 0x2, 0x388, 0x38a, 
    0x7, 0x77, 0x2, 0x2, 0x389, 0x388, 0x3, 0x2, 0x2, 0x2, 0x389, 0x38a, 
    0x3, 0x2, 0x2, 0x2, 0x38a, 0x38c, 0x3, 0x2, 0x2, 0x2, 0x38b, 0x387, 
    0x3, 0x2, 0x2, 0x2, 0x38b, 0x38c, 0x3, 0x2, 0x2, 0x2, 0x38c, 0x38d, 
    0x3, 0x2, 0x2, 0x2, 0x38d, 0x38f, 0x8, 0x3a, 0x1, 0x2, 0x38e, 0x361, 
    0x3, 0x2, 0x2, 0x2, 0x38e, 0x379, 0x3, 0x2, 0x2, 0x2, 0x38f, 0x73, 0x3, 
    0x2, 0x2, 0x2, 0x390, 0x392, 0x5, 0x76, 0x3c, 0x2, 0x391, 0x393, 0x7, 
    0x77, 0x2, 0x2, 0x392, 0x391, 0x3, 0x2, 0x2, 0x2, 0x392, 0x393, 0x3, 
    0x2, 0x2, 0x2, 0x393, 0x394, 0x3, 0x2, 0x2, 0x2, 0x394, 0x395, 0x5, 
    0x72, 0x3a, 0x2, 0x395, 0x75, 0x3, 0x2, 0x2, 0x2, 0x396, 0x398, 0x5, 
    0xe0, 0x71, 0x2, 0x397, 0x399, 0x7, 0x77, 0x2, 0x2, 0x398, 0x397, 0x3, 
    0x2, 0x2, 0x2, 0x398, 0x399, 0x3, 0x2, 0x2, 0x2, 0x399, 0x39a, 0x3, 
    0x2, 0x2, 0x2, 0x39a, 0x39c, 0x5, 0xe4, 0x73, 0x2, 0x39b, 0x39d, 0x7, 
    0x77, 0x2, 0x2, 0x39c, 0x39b, 0x3, 0x2, 0x2, 0x2, 0x39c, 0x39d, 0x3, 
    0x2, 0x2, 0x2, 0x39d, 0x39f, 0x3, 0x2, 0x2, 0x2, 0x39e, 0x3a0, 0x5, 
    0x78, 0x3d, 0x2, 0x39f, 0x39e, 0x3, 0x2, 0x2, 0x2, 0x39f, 0x3a0, 0x3, 
    0x2, 0x2, 0x2, 0x3a0, 0x3a2, 0x3, 0x2, 0x2, 0x2, 0x3a1, 0x3a3, 0x7, 
    0x77, 0x2, 0x2, 0x3a2, 0x3a1, 0x3, 0x2, 0x2, 0x2, 0x3a2, 0x3a3, 0x3, 
    0x2, 0x2, 0x2, 0x3a3, 0x3a4, 0x3, 0x2, 0x2, 0x2, 0x3a4, 0x3a5, 0x5, 
    0xe4, 0x73, 0x2, 0x3a5, 0x3b7, 0x3, 0x2, 0x2, 0x2, 0x3a6, 0x3a8, 0x5, 
    0xe4, 0x73, 0x2, 0x3a7, 0x3a9, 0x7, 0x77, 0x2, 0x2, 0x3a8, 0x3a7, 0x3, 
    0x2, 0x2, 0x2, 0x3a8, 0x3a9, 0x3, 0x2, 0x2, 0x2, 0x3a9, 0x3ab, 0x3, 
    0x2, 0x2, 0x2, 0x3aa, 0x3ac, 0x5, 0x78, 0x3d, 0x2, 0x3ab, 0x3aa, 0x3, 
    0x2, 0x2, 0x2, 0x3ab, 0x3ac, 0x3, 0x2, 0x2, 0x2, 0x3ac, 0x3ae, 0x3, 
    0x2, 0x2, 0x2, 0x3ad, 0x3af, 0x7, 0x77, 0x2, 0x2, 0x3ae, 0x3ad, 0x3, 
    0x2, 0x2, 0x2, 0x3ae, 0x3af, 0x3, 0x2, 0x2, 0x2, 0x3af, 0x3b0, 0x3, 
    0x2, 0x2, 0x2, 0x3b0, 0x3b2, 0x5, 0xe4, 0x73, 0x2, 0x3b1, 0x3b3, 0x7, 
    0x77, 0x2, 0x2, 0x3b2, 0x3b1, 0x3, 0x2, 0x2, 0x2, 0x3b2, 0x3b3, 0x3, 
    0x2, 0x2, 0x2, 0x3b3, 0x3b4, 0x3, 0x2, 0x2, 0x2, 0x3b4, 0x3b5, 0x5, 
    0xe2, 0x72, 0x2, 0x3b5, 0x3b7, 0x3, 0x2, 0x2, 0x2, 0x3b6, 0x396, 0x3, 
    0x2, 0x2, 0x2, 0x3b6, 0x3a6, 0x3, 0x2, 0x2, 0x2, 0x3b7, 0x77, 0x3, 0x2, 
    0x2, 0x2, 0x3b8, 0x3ba, 0x7, 0x9, 0x2, 0x2, 0x3b9, 0x3bb, 0x7, 0x77, 
    0x2, 0x2, 0x3ba, 0x3b9, 0x3, 0x2, 0x2, 0x2, 0x3ba, 0x3bb, 0x3, 0x2, 
    0x2, 0x2, 0x3bb, 0x3c0, 0x3, 0x2, 0x2, 0x2, 0x3bc, 0x3be, 0x5, 0xce, 
    0x68, 0x2, 0x3bd, 0x3bf, 0x7, 0x77, 0x2, 0x2, 0x3be, 0x3bd, 0x3, 0x2, 
    0x2, 0x2, 0x3be, 0x3bf, 0x3, 0x2, 0x2, 0x2, 0x3bf, 0x3c1, 0x3, 0x2, 
    0x2, 0x2, 0x3c0, 0x3bc, 0x3, 0x2, 0x2, 0x2, 0x3c0, 0x3c1, 0x3, 0x2, 
    0x2, 0x2, 0x3c1, 0x3c6, 0x3, 0x2, 0x2, 0x2, 0x3c2, 0x3c4, 0x5, 0x7c, 
    0x3f, 0x2, 0x3c3, 0x3c5, 0x7, 0x77, 0x2, 0x2, 0x3c4, 0x3c3, 0x3, 0x2, 
    0x2, 0x2, 0x3c4, 0x3c5, 0x3, 0x2, 0x2, 0x2, 0x3c5, 0x3c7, 0x3, 0x2, 
    0x2, 0x2, 0x3c6, 0x3c2, 0x3, 0x2, 0x2, 0x2, 0x3c6, 0x3c7, 0x3, 0x2, 
    0x2, 0x2, 0x3c7, 0x3cc, 0x3, 0x2, 0x2, 0x2, 0x3c8, 0x3ca, 0x5, 0x82, 
    0x42, 0x2, 0x3c9, 0x3cb, 0x7, 0x77, 0x2, 0x2, 0x3ca, 0x3c9, 0x3, 0x2, 
    0x2, 0x2, 0x3ca, 0x3cb, 0x3, 0x2, 0x2, 0x2, 0x3cb, 0x3cd, 0x3, 0x2, 
    0x2, 0x2, 0x3cc, 0x3c8, 0x3, 0x2, 0x2, 0x2, 0x3cc, 0x3cd, 0x3, 0x2, 
    0x2, 0x2, 0x3cd, 0x3d2, 0x3, 0x2, 0x2, 0x2, 0x3ce, 0x3d0, 0x5, 0x7a, 
    0x3e, 0x2, 0x3cf, 0x3d1, 0x7, 0x77, 0x2, 0x2, 0x3d0, 0x3cf, 0x3, 0x2, 
    0x2, 0x2, 0x3d0, 0x3d1, 0x3, 0x2, 0x2, 0x2, 0x3d1, 0x3d3, 0x3, 0x2, 
    0x2, 0x2, 0x3d2, 0x3ce, 0x3, 0x2, 0x2, 0x2, 0x3d2, 0x3d3, 0x3, 0x2, 
    0x2, 0x2, 0x3d3, 0x3d4, 0x3, 0x2, 0x2, 0x2, 0x3d4, 0x3d5, 0x7, 0xa, 
    0x2, 0x2, 0x3d5, 0x79, 0x3, 0x2, 0x2, 0x2, 0x3d6, 0x3d8, 0x7, 0xb, 0x2, 
    0x2, 0x3d7, 0x3d9, 0x7, 0x77, 0x2, 0x2, 0x3d8, 0x3d7, 0x3, 0x2, 0x2, 
    0x2, 0x3d8, 0x3d9, 0x3, 0x2, 0x2, 0x2, 0x3d9, 0x3fb, 0x3, 0x2, 0x2, 
    0x2, 0x3da, 0x3dc, 0x5, 0xd6, 0x6c, 0x2, 0x3db, 0x3dd, 0x7, 0x77, 0x2, 
    0x2, 0x3dc, 0x3db, 0x3, 0x2, 0x2, 0x2, 0x3dc, 0x3dd, 0x3, 0x2, 0x2, 
    0x2, 0x3dd, 0x3de, 0x3, 0x2, 0x2, 0x2, 0x3de, 0x3e0, 0x7, 0xc, 0x2, 
    0x2, 0x3df, 0x3e1, 0x7, 0x77, 0x2, 0x2, 0x3e0, 0x3df, 0x3, 0x2, 0x2, 
    0x2, 0x3e0, 0x3e1, 0x3, 0x2, 0x2, 0x2, 0x3e1, 0x3e2, 0x3, 0x2, 0x2, 
    0x2, 0x3e2, 0x3e4, 0x5, 0x88, 0x45, 0x2, 0x3e3, 0x3e5, 0x7, 0x77, 0x2, 
    0x2, 0x3e4, 0x3e3, 0x3, 0x2, 0x2, 0x2, 0x3e4, 0x3e5, 0x3, 0x2, 0x2, 
    0x2, 0x3e5, 0x3f8, 0x3, 0x2, 0x2, 0x2, 0x3e6, 0x3e8, 0x7, 0x6, 0x2, 
    0x2, 0x3e7, 0x3e9, 0x7, 0x77, 0x2, 0x2, 0x3e8, 0x3e7, 0x3, 0x2, 0x2, 
    0x2, 0x3e8, 0x3e9, 0x3, 0x2, 0x2, 0x2, 0x3e9, 0x3ea, 0x3, 0x2, 0x2, 
    0x2, 0x3ea, 0x3ec, 0x5, 0xd6, 0x6c, 0x2, 0x3eb, 0x3ed, 0x7, 0x77, 0x2, 
    0x2, 0x3ec, 0x3eb, 0x3, 0x2, 0x2, 0x2, 0x3ec, 0x3ed, 0x3, 0x2, 0x2, 
    0x2, 0x3ed, 0x3ee, 0x3, 0x2, 0x2, 0x2, 0x3ee, 0x3f0, 0x7, 0xc, 0x2, 
    0x2, 0x3ef, 0x3f1, 0x7, 0x77, 0x2, 0x2, 0x3f0, 0x3ef, 0x3, 0x2, 0x2, 
    0x2, 0x3f0, 0x3f1, 0x3, 0x2, 0x2, 0x2, 0x3f1, 0x3f2, 0x3, 0x2, 0x2, 
    0x2, 0x3f2, 0x3f4, 0x5, 0x88, 0x45, 0x2, 0x3f3, 0x3f5, 0x7, 0x77, 0x2, 
    0x2, 0x3f4, 0x3f3, 0x3, 0x2, 0x2, 0x2, 0x3f4, 0x3f5, 0x3, 0x2, 0x2, 
    0x2, 0x3f5, 0x3f7, 0x3, 0x2, 0x2, 0x2, 0x3f6, 0x3e6, 0x3, 0x2, 0x2, 
    0x2, 0x3f7, 0x3fa, 0x3, 0x2, 0x2, 0x2, 0x3f8, 0x3f6, 0x3, 0x2, 0x2, 
    0x2, 0x3f8, 0x3f9, 0x3, 0x2, 0x2, 0x2, 0x3f9, 0x3fc, 0x3, 0x2, 0x2, 
    0x2, 0x3fa, 0x3f8, 0x3, 0x2, 0x2, 0x2, 0x3fb, 0x3da, 0x3, 0x2, 0x2, 
    0x2, 0x3fb, 0x3fc, 0x3, 0x2, 0x2, 0x2, 0x3fc, 0x3fd, 0x3, 0x2, 0x2, 
    0x2, 0x3fd, 0x3fe, 0x7, 0xd, 0x2, 0x2, 0x3fe, 0x7b, 0x3, 0x2, 0x2, 0x2, 
    0x3ff, 0x401, 0x7, 0xc, 0x2, 0x2, 0x400, 0x402, 0x7, 0x77, 0x2, 0x2, 
    0x401, 0x400, 0x3, 0x2, 0x2, 0x2, 0x401, 0x402, 0x3, 0x2, 0x2, 0x2, 
    0x402, 0x403, 0x3, 0x2, 0x2, 0x2, 0x403, 0x411, 0x5, 0x86, 0x44, 0x2, 
    0x404, 0x406, 0x7, 0x77, 0x2, 0x2, 0x405, 0x404, 0x3, 0x2, 0x2, 0x2, 
    0x405, 0x406, 0x3, 0x2, 0x2, 0x2, 0x406, 0x407, 0x3, 0x2, 0x2, 0x2, 
    0x407, 0x409, 0x7, 0x8, 0x2, 0x2, 0x408, 0x40a, 0x7, 0xc, 0x2, 0x2, 
    0x409, 0x408, 0x3, 0x2, 0x2, 0x2, 0x409, 0x40a, 0x3, 0x2, 0x2, 0x2, 
    0x40a, 0x40c, 0x3, 0x2, 0x2, 0x2, 0x40b, 0x40d, 0x7, 0x77, 0x2, 0x2, 
    0x40c, 0x40b, 0x3, 0x2, 0x2, 0x2, 0x40c, 0x40d, 0x3, 0x2, 0x2, 0x2, 
    0x40d, 0x40e, 0x3, 0x2, 0x2, 0x2, 0x40e, 0x410, 0x5, 0x86, 0x44, 0x2, 
    0x40f, 0x405, 0x3, 0x2, 0x2, 0x2, 0x410, 0x413, 0x3, 0x2, 0x2, 0x2, 
    0x411, 0x40f, 0x3, 0x2, 0x2, 0x2, 0x411, 0x412, 0x3, 0x2, 0x2, 0x2, 
    0x412, 0x7d, 0x3, 0x2, 0x2, 0x2, 0x413, 0x411, 0x3, 0x2, 0x2, 0x2, 0x414, 
    0x41b, 0x5, 0x80, 0x41, 0x2, 0x415, 0x417, 0x7, 0x77, 0x2, 0x2, 0x416, 
    0x415, 0x3, 0x2, 0x2, 0x2, 0x416, 0x417, 0x3, 0x2, 0x2, 0x2, 0x417, 
    0x418, 0x3, 0x2, 0x2, 0x2, 0x418, 0x41a, 0x5, 0x80, 0x41, 0x2, 0x419, 
    0x416, 0x3, 0x2, 0x2, 0x2, 0x41a, 0x41d, 0x3, 0x2, 0x2, 0x2, 0x41b, 
    0x419, 0x3, 0x2, 0x2, 0x2, 0x41b, 0x41c, 0x3, 0x2, 0x2, 0x2, 0x41c, 
    0x7f, 0x3, 0x2, 0x2, 0x2, 0x41d, 0x41b, 0x3, 0x2, 0x2, 0x2, 0x41e, 0x420, 
    0x7, 0xc, 0x2, 0x2, 0x41f, 0x421, 0x7, 0x77, 0x2, 0x2, 0x420, 0x41f, 
    0x3, 0x2, 0x2, 0x2, 0x420, 0x421, 0x3, 0x2, 0x2, 0x2, 0x421, 0x422, 
    0x3, 0x2, 0x2, 0x2, 0x422, 0x423, 0x5, 0x84, 0x43, 0x2, 0x423, 0x81, 
    0x3, 0x2, 0x2, 0x2, 0x424, 0x426, 0x7, 0x4a, 0x2, 0x2, 0x425, 0x427, 
    0x7, 0x77, 0x2, 0x2, 0x426, 0x425, 0x3, 0x2, 0x2, 0x2, 0x426, 0x427, 
    0x3, 0x2, 0x2, 0x2, 0x427, 0x428, 0x3, 0x2, 0x2, 0x2, 0x428, 0x42a, 
    0x5, 0xd8, 0x6d, 0x2, 0x429, 0x42b, 0x7, 0x77, 0x2, 0x2, 0x42a, 0x429, 
    0x3, 0x2, 0x2, 0x2, 0x42a, 0x42b, 0x3, 0x2, 0x2, 0x2, 0x42b, 0x42c, 
    0x3, 0x2, 0x2, 0x2, 0x42c, 0x42e, 0x7, 0xe, 0x2, 0x2, 0x42d, 0x42f, 
    0x7, 0x77, 0x2, 0x2, 0x42e, 0x42d, 0x3, 0x2, 0x2, 0x2, 0x42e, 0x42f, 
    0x3, 0x2, 0x2, 0x2, 0x42f, 0x430, 0x3, 0x2, 0x2, 0x2, 0x430, 0x431, 
    0x5, 0xd8, 0x6d, 0x2, 0x431, 0x83, 0x3, 0x2, 0x2, 0x2, 0x432, 0x433, 
    0x5, 0xdc, 0x6f, 0x2, 0x433, 0x85, 0x3, 0x2, 0x2, 0x2, 0x434, 0x435, 
    0x5, 0xdc, 0x6f, 0x2, 0x435, 0x87, 0x3, 0x2, 0x2, 0x2, 0x436, 0x437, 
    0x5, 0x8a, 0x46, 0x2, 0x437, 0x89, 0x3, 0x2, 0x2, 0x2, 0x438, 0x43f, 
    0x5, 0x8c, 0x47, 0x2, 0x439, 0x43a, 0x7, 0x77, 0x2, 0x2, 0x43a, 0x43b, 
    0x7, 0x55, 0x2, 0x2, 0x43b, 0x43c, 0x7, 0x77, 0x2, 0x2, 0x43c, 0x43e, 
    0x5, 0x8c, 0x47, 0x2, 0x43d, 0x439, 0x3, 0x2, 0x2, 0x2, 0x43e, 0x441, 
    0x3, 0x2, 0x2, 0x2, 0x43f, 0x43d, 0x3, 0x2, 0x2, 0x2, 0x43f, 0x440, 
    0x3, 0x2, 0x2, 0x2, 0x440, 0x8b, 0x3, 0x2, 0x2, 0x2, 0x441, 0x43f, 0x3, 
    0x2, 0x2, 0x2, 0x442, 0x449, 0x5, 0x8e, 0x48, 0x2, 0x443, 0x444, 0x7, 
    0x77, 0x2, 0x2, 0x444, 0x445, 0x7, 0x56, 0x2, 0x2, 0x445, 0x446, 0x7, 
    0x77, 0x2, 0x2, 0x446, 0x448, 0x5, 0x8e, 0x48, 0x2, 0x447, 0x443, 0x3, 
    0x2, 0x2, 0x2, 0x448, 0x44b, 0x3, 0x2, 0x2, 0x2, 0x449, 0x447, 0x3, 
    0x2, 0x2, 0x2, 0x449, 0x44a, 0x3, 0x2, 0x2, 0x2, 0x44a, 0x8d, 0x3, 0x2, 
    0x2, 0x2, 0x44b, 0x449, 0x3, 0x2, 0x2, 0x2, 0x44c, 0x453, 0x5, 0x90, 
    0x49, 0x2, 0x44d, 0x44e, 0x7, 0x77, 0x2, 0x2, 0x44e, 0x44f, 0x7, 0x57, 
    0x2, 0x2, 0x44f, 0x450, 0x7, 0x77, 0x2, 0x2, 0x450, 0x452, 0x5, 0x90, 
    0x49, 0x2, 0x451, 0x44d, 0x3, 0x2, 0x2, 0x2, 0x452, 0x455, 0x3, 0x2, 
    0x2, 0x2, 0x453, 0x451, 0x3, 0x2, 0x2, 0x2, 0x453, 0x454, 0x3, 0x2, 
    0x2, 0x2, 0x454, 0x8f, 0x3, 0x2, 0x2, 0x2, 0x455, 0x453, 0x3, 0x2, 0x2, 
    0x2, 0x456, 0x458, 0x7, 0x58, 0x2, 0x2, 0x457, 0x459, 0x7, 0x77, 0x2, 
    0x2, 0x458, 0x457, 0x3, 0x2, 0x2, 0x2, 0x458, 0x459, 0x3, 0x2, 0x2, 
    0x2, 0x459, 0x45b, 0x3, 0x2, 0x2, 0x2, 0x45a, 0x456, 0x3, 0x2, 0x2, 
    0x2, 0x45a, 0x45b, 0x3, 0x2, 0x2, 0x2, 0x45b, 0x45c, 0x3, 0x2, 0x2, 
    0x2, 0x45c, 0x45d, 0x5, 0x92, 0x4a, 0x2, 0x45d, 0x91, 0x3, 0x2, 0x2, 
    0x2, 0x45e, 0x468, 0x5, 0x96, 0x4c, 0x2, 0x45f, 0x461, 0x7, 0x77, 0x2, 
    0x2, 0x460, 0x45f, 0x3, 0x2, 0x2, 0x2, 0x460, 0x461, 0x3, 0x2, 0x2, 
    0x2, 0x461, 0x462, 0x3, 0x2, 0x2, 0x2, 0x462, 0x464, 0x5, 0x94, 0x4b, 
    0x2, 0x463, 0x465, 0x7, 0x77, 0x2, 0x2, 0x464, 0x463, 0x3, 0x2, 0x2, 
    0x2, 0x464, 0x465, 0x3, 0x2, 0x2, 0x2, 0x465, 0x466, 0x3, 0x2, 0x2, 
    0x2, 0x466, 0x467, 0x5, 0x96, 0x4c, 0x2, 0x467, 0x469, 0x3, 0x2, 0x2, 
    0x2, 0x468, 0x460, 0x3, 0x2, 0x2, 0x2, 0x468, 0x469, 0x3, 0x2, 0x2, 
    0x2, 0x469, 0x48f, 0x3, 0x2, 0x2, 0x2, 0x46a, 0x46c, 0x5, 0x96, 0x4c, 
    0x2, 0x46b, 0x46d, 0x7, 0x77, 0x2, 0x2, 0x46c, 0x46b, 0x3, 0x2, 0x2, 
    0x2, 0x46c, 0x46d, 0x3, 0x2, 0x2, 0x2, 0x46d, 0x46e, 0x3, 0x2, 0x2, 
    0x2, 0x46e, 0x470, 0x7, 0x59, 0x2, 0x2, 0x46f, 0x471, 0x7, 0x77, 0x2, 
    0x2, 0x470, 0x46f, 0x3, 0x2, 0x2, 0x2, 0x470, 0x471, 0x3, 0x2, 0x2, 
    0x2, 0x471, 0x472, 0x3, 0x2, 0x2, 0x2, 0x472, 0x473, 0x5, 0x96, 0x4c, 
    0x2, 0x473, 0x474, 0x3, 0x2, 0x2, 0x2, 0x474, 0x475, 0x8, 0x4a, 0x1, 
    0x2, 0x475, 0x48f, 0x3, 0x2, 0x2, 0x2, 0x476, 0x478, 0x5, 0x96, 0x4c, 
    0x2, 0x477, 0x479, 0x7, 0x77, 0x2, 0x2, 0x478, 0x477, 0x3, 0x2, 0x2, 
    0x2, 0x478, 0x479, 0x3, 0x2, 0x2, 0x2, 0x479, 0x47a, 0x3, 0x2, 0x2, 
    0x2, 0x47a, 0x47c, 0x5, 0x94, 0x4b, 0x2, 0x47b, 0x47d, 0x7, 0x77, 0x2, 
    0x2, 0x47c, 0x47b, 0x3, 0x2, 0x2, 0x2, 0x47c, 0x47d, 0x3, 0x2, 0x2, 
    0x2, 0x47d, 0x47e, 0x3, 0x2, 0x2, 0x2, 0x47e, 0x488, 0x5, 0x96, 0x4c, 
    0x2, 0x47f, 0x481, 0x7, 0x77, 0x2, 0x2, 0x480, 0x47f, 0x3, 0x2, 0x2, 
    0x2, 0x480, 0x481, 0x3, 0x2, 0x2, 0x2, 0x481, 0x482, 0x3, 0x2, 0x2, 
    0x2, 0x482, 0x484, 0x5, 0x94, 0x4b, 0x2, 0x483, 0x485, 0x7, 0x77, 0x2, 
    0x2, 0x484, 0x483, 0x3, 0x2, 0x2, 0x2, 0x484, 0x485, 0x3, 0x2, 0x2, 
    0x2, 0x485, 0x486, 0x3, 0x2, 0x2, 0x2, 0x486, 0x487, 0x5, 0x96, 0x4c, 
    0x2, 0x487, 0x489, 0x3, 0x2, 0x2, 0x2, 0x488, 0x480, 0x3, 0x2, 0x2, 
    0x2, 0x489, 0x48a, 0x3, 0x2, 0x2, 0x2, 0x48a, 0x488, 0x3, 0x2, 0x2, 
    0x2, 0x48a, 0x48b, 0x3, 0x2, 0x2, 0x2, 0x48b, 0x48c, 0x3, 0x2, 0x2, 
    0x2, 0x48c, 0x48d, 0x8, 0x4a, 0x1, 0x2, 0x48d, 0x48f, 0x3, 0x2, 0x2, 
    0x2, 0x48e, 0x45e, 0x3, 0x2, 0x2, 0x2, 0x48e, 0x46a, 0x3, 0x2, 0x2, 
    0x2, 0x48e, 0x476, 0x3, 0x2, 0x2, 0x2, 0x48f, 0x93, 0x3, 0x2, 0x2, 0x2, 
    0x490, 0x491, 0x9, 0x3, 0x2, 0x2, 0x491, 0x95, 0x3, 0x2, 0x2, 0x2, 0x492, 
    0x49d, 0x5, 0x98, 0x4d, 0x2, 0x493, 0x495, 0x7, 0x77, 0x2, 0x2, 0x494, 
    0x493, 0x3, 0x2, 0x2, 0x2, 0x494, 0x495, 0x3, 0x2, 0x2, 0x2, 0x495, 
    0x496, 0x3, 0x2, 0x2, 0x2, 0x496, 0x498, 0x7, 0x8, 0x2, 0x2, 0x497, 
    0x499, 0x7, 0x77, 0x2, 0x2, 0x498, 0x497, 0x3, 0x2, 0x2, 0x2, 0x498, 
    0x499, 0x3, 0x2, 0x2, 0x2, 0x499, 0x49a, 0x3, 0x2, 0x2, 0x2, 0x49a, 
    0x49c, 0x5, 0x98, 0x4d, 0x2, 0x49b, 0x494, 0x3, 0x2, 0x2, 0x2, 0x49c, 
    0x49f, 0x3, 0x2, 0x2, 0x2, 0x49d, 0x49b, 0x3, 0x2, 0x2, 0x2, 0x49d, 
    0x49e, 0x3, 0x2, 0x2, 0x2, 0x49e, 0x97, 0x3, 0x2, 0x2, 0x2, 0x49f, 0x49d, 
    0x3, 0x2, 0x2, 0x2, 0x4a0, 0x4ab, 0x5, 0x9a, 0x4e, 0x2, 0x4a1, 0x4a3, 
    0x7, 0x77, 0x2, 0x2, 0x4a2, 0x4a1, 0x3, 0x2, 0x2, 0x2, 0x4a2, 0x4a3, 
    0x3, 0x2, 0x2, 0x2, 0x4a3, 0x4a4, 0x3, 0x2, 0x2, 0x2, 0x4a4, 0x4a6, 
    0x7, 0x14, 0x2, 0x2, 0x4a5, 0x4a7, 0x7, 0x77, 0x2, 0x2, 0x4a6, 0x4a5, 
    0x3, 0x2, 0x2, 0x2, 0x4a6, 0x4a7, 0x3, 0x2, 0x2, 0x2, 0x4a7, 0x4a8, 
    0x3, 0x2, 0x2, 0x2, 0x4a8, 0x4aa, 0x5, 0x9a, 0x4e, 0x2, 0x4a9, 0x4a2, 
    0x3, 0x2, 0x2, 0x2, 0x4aa, 0x4ad, 0x3, 0x2, 0x2, 0x2, 0x4ab, 0x4a9, 
    0x3, 0x2, 0x2, 0x2, 0x4ab, 0x4ac, 0x3, 0x2, 0x2, 0x2, 0x4ac, 0x99, 0x3, 
    0x2, 0x2, 0x2, 0x4ad, 0x4ab, 0x3, 0x2, 0x2, 0x2, 0x4ae, 0x4ba, 0x5, 
    0x9e, 0x50, 0x2, 0x4af, 0x4b1, 0x7, 0x77, 0x2, 0x2, 0x4b0, 0x4af, 0x3, 
    0x2, 0x2, 0x2, 0x4b0, 0x4b1, 0x3, 0x2, 0x2, 0x2, 0x4b1, 0x4b2, 0x3, 
    0x2, 0x2, 0x2, 0x4b2, 0x4b4, 0x5, 0x9c, 0x4f, 0x2, 0x4b3, 0x4b5, 0x7, 
    0x77, 0x2, 0x2, 0x4b4, 0x4b3, 0x3, 0x2, 0x2, 0x2, 0x4b4, 0x4b5, 0x3, 
    0x2, 0x2, 0x2, 0x4b5, 0x4b6, 0x3, 0x2, 0x2, 0x2, 0x4b6, 0x4b7, 0x5, 
    0x9e, 0x50, 0x2, 0x4b7, 0x4b9, 0x3, 0x2, 0x2, 0x2, 0x4b8, 0x4b0, 0x3, 
    0x2, 0x2, 0x2, 0x4b9, 0x4bc, 0x3, 0x2, 0x2, 0x2, 0x4ba, 0x4b8, 0x3, 
    0x2, 0x2, 0x2, 0x4ba, 0x4bb, 0x3, 0x2, 0x2, 0x2, 0x4bb, 0x9b, 0x3, 0x2, 
    0x2, 0x2, 0x4bc, 0x4ba, 0x3, 0x2, 0x2, 0x2, 0x4bd, 0x4be, 0x9, 0x4, 
    0x2, 0x2, 0x4be, 0x9d, 0x3, 0x2, 0x2, 0x2, 0x4bf, 0x4cb, 0x5, 0xa2, 
    0x52, 0x2, 0x4c0, 0x4c2, 0x7, 0x77, 0x2, 0x2, 0x4c1, 0x4c0, 0x3, 0x2, 
    0x2, 0x2, 0x4c1, 0x4c2, 0x3, 0x2, 0x2, 0x2, 0x4c2, 0x4c3, 0x3, 0x2, 
    0x2, 0x2, 0x4c3, 0x4c5, 0x5, 0xa0, 0x51, 0x2, 0x4c4, 0x4c6, 0x7, 0x77, 
    0x2, 0x2, 0x4c5, 0x4c4, 0x3, 0x2, 0x2, 0x2, 0x4c5, 0x4c6, 0x3, 0x2, 
    0x2, 0x2, 0x4c6, 0x4c7, 0x3, 0x2, 0x2, 0x2, 0x4c7, 0x4c8, 0x5, 0xa2, 
    0x52, 0x2, 0x4c8, 0x4ca, 0x3, 0x2, 0x2, 0x2, 0x4c9, 0x4c1, 0x3, 0x2, 
    0x2, 0x2, 0x4ca, 0x4cd, 0x3, 0x2, 0x2, 0x2, 0x4cb, 0x4c9, 0x3, 0x2, 
    0x2, 0x2, 0x4cb, 0x4cc, 0x3, 0x2, 0x2, 0x2, 0x4cc, 0x9f, 0x3, 0x2, 0x2, 
    0x2, 0x4cd, 0x4cb, 0x3, 0x2, 0x2, 0x2, 0x4ce, 0x4cf, 0x9, 0x5, 0x2, 
    0x2, 0x4cf, 0xa1, 0x3, 0x2, 0x2, 0x2, 0x4d0, 0x4dc, 0x5, 0xa6, 0x54, 
    0x2, 0x4d1, 0x4d3, 0x7, 0x77, 0x2, 0x2, 0x4d2, 0x4d1, 0x3, 0x2, 0x2, 
    0x2, 0x4d2, 0x4d3, 0x3, 0x2, 0x2, 0x2, 0x4d3, 0x4d4, 0x3, 0x2, 0x2, 
    0x2, 0x4d4, 0x4d6, 0x5, 0xa4, 0x53, 0x2, 0x4d5, 0x4d7, 0x7, 0x77, 0x2, 
    0x2, 0x4d6, 0x4d5, 0x3, 0x2, 0x2, 0x2, 0x4d6, 0x4d7, 0x3, 0x2, 0x2, 
    0x2, 0x4d7, 0x4d8, 0x3, 0x2, 0x2, 0x2, 0x4d8, 0x4d9, 0x5, 0xa6, 0x54, 
    0x2, 0x4d9, 0x4db, 0x3, 0x2, 0x2, 0x2, 0x4da, 0x4d2, 0x3, 0x2, 0x2, 
    0x2, 0x4db, 0x4de, 0x3, 0x2, 0x2, 0x2, 0x4dc, 0x4da, 0x3, 0x2, 0x2, 
    0x2, 0x4dc, 0x4dd, 0x3, 0x2, 0x2, 0x2, 0x4dd, 0xa3, 0x3, 0x2, 0x2, 0x2, 
    0x4de, 0x4dc, 0x3, 0x2, 0x2, 0x2, 0x4df, 0x4e0, 0x9, 0x6, 0x2, 0x2, 
    0x4e0, 0xa5, 0x3, 0x2, 0x2, 0x2, 0x4e1, 0x4ec, 0x5, 0xa8, 0x55, 0x2, 
    0x4e2, 0x4e4, 0x7, 0x77, 0x2, 0x2, 0x4e3, 0x4e2, 0x3, 0x2, 0x2, 0x2, 
    0x4e3, 0x4e4, 0x3, 0x2, 0x2, 0x2, 0x4e4, 0x4e5, 0x3, 0x2, 0x2, 0x2, 
    0x4e5, 0x4e7, 0x7, 0x1a, 0x2, 0x2, 0x4e6, 0x4e8, 0x7, 0x77, 0x2, 0x2, 
    0x4e7, 0x4e6, 0x3, 0x2, 0x2, 0x2, 0x4e7, 0x4e8, 0x3, 0x2, 0x2, 0x2, 
    0x4e8, 0x4e9, 0x3, 0x2, 0x2, 0x2, 0x4e9, 0x4eb, 0x5, 0xa8, 0x55, 0x2, 
    0x4ea, 0x4e3, 0x3, 0x2, 0x2, 0x2, 0x4eb, 0x4ee, 0x3, 0x2, 0x2, 0x2, 
    0x4ec, 0x4ea, 0x3, 0x2, 0x2, 0x2, 0x4ec, 0x4ed, 0x3, 0x2, 0x2, 0x2, 
    0x4ed, 0xa7, 0x3, 0x2, 0x2, 0x2, 0x4ee, 0x4ec, 0x3, 0x2, 0x2, 0x2, 0x4ef, 
    0x4f1, 0x7, 0x5a, 0x2, 0x2, 0x4f0, 0x4f2, 0x7, 0x77, 0x2, 0x2, 0x4f1, 
    0x4f0, 0x3, 0x2, 0x2, 0x2, 0x4f1, 0x4f2, 0x3, 0x2, 0x2, 0x2, 0x4f2, 
    0x4f4, 0x3, 0x2, 0x2, 0x2, 0x4f3, 0x4ef, 0x3, 0x2, 0x2, 0x2, 0x4f3, 
    0x4f4, 0x3, 0x2, 0x2, 0x2, 0x4f4, 0x4f5, 0x3, 0x2, 0x2, 0x2, 0x4f5, 
    0x4fa, 0x5, 0xaa, 0x56, 0x2, 0x4f6, 0x4f8, 0x7, 0x77, 0x2, 0x2, 0x4f7, 
    0x4f6, 0x3, 0x2, 0x2, 0x2, 0x4f7, 0x4f8, 0x3, 0x2, 0x2, 0x2, 0x4f8, 
    0x4f9, 0x3, 0x2, 0x2, 0x2, 0x4f9, 0x4fb, 0x7, 0x5b, 0x2, 0x2, 0x4fa, 
    0x4f7, 0x3, 0x2, 0x2, 0x2, 0x4fa, 0x4fb, 0x3, 0x2, 0x2, 0x2, 0x4fb, 
    0xa9, 0x3, 0x2, 0x2, 0x2, 0x4fc, 0x500, 0x5, 0xb6, 0x5c, 0x2, 0x4fd, 
    0x501, 0x5, 0xb2, 0x5a, 0x2, 0x4fe, 0x501, 0x5, 0xac, 0x57, 0x2, 0x4ff, 
    0x501, 0x5, 0xb4, 0x5b, 0x2, 0x500, 0x4fd, 0x3, 0x2, 0x2, 0x2, 0x500, 
    0x4fe, 0x3, 0x2, 0x2, 0x2, 0x500, 0x4ff, 0x3, 0x2, 0x2, 0x2, 0x500, 
    0x501, 0x3, 0x2, 0x2, 0x2, 0x501, 0xab, 0x3, 0x2, 0x2, 0x2, 0x502, 0x505, 
    0x5, 0xae, 0x58, 0x2, 0x503, 0x505, 0x5, 0xb0, 0x59, 0x2, 0x504, 0x502, 
    0x3, 0x2, 0x2, 0x2, 0x504, 0x503, 0x3, 0x2, 0x2, 0x2, 0x505, 0x507, 
    0x3, 0x2, 0x2, 0x2, 0x506, 0x508, 0x5, 0xac, 0x57, 0x2, 0x507, 0x506, 
    0x3, 0x2, 0x2, 0x2, 0x507, 0x508, 0x3, 0x2, 0x2, 0x2, 0x508, 0xad, 0x3, 
    0x2, 0x2, 0x2, 0x509, 0x50b, 0x7, 0x77, 0x2, 0x2, 0x50a, 0x509, 0x3, 
    0x2, 0x2, 0x2, 0x50a, 0x50b, 0x3, 0x2, 0x2, 0x2, 0x50b, 0x50c, 0x3, 
    0x2, 0x2, 0x2, 0x50c, 0x50d, 0x7, 0x9, 0x2, 0x2, 0x50d, 0x50e, 0x5, 
    0x88, 0x45, 0x2, 0x50e, 0x50f, 0x7, 0xa, 0x2, 0x2, 0x50f, 0xaf, 0x3, 
    0x2, 0x2, 0x2, 0x510, 0x512, 0x7, 0x77, 0x2, 0x2, 0x511, 0x510, 0x3, 
    0x2, 0x2, 0x2, 0x511, 0x512, 0x3, 0x2, 0x2, 0x2, 0x512, 0x513, 0x3, 
    0x2, 0x2, 0x2, 0x513, 0x515, 0x7, 0x9, 0x2, 0x2, 0x514, 0x516, 0x5, 
    0x88, 0x45, 0x2, 0x515, 0x514, 0x3, 0x2, 0x2, 0x2, 0x515, 0x516, 0x3, 
    0x2, 0x2, 0x2, 0x516, 0x517, 0x3, 0x2, 0x2, 0x2, 0x517, 0x519, 0x7, 
    0xc, 0x2, 0x2, 0x518, 0x51a, 0x5, 0x88, 0x45, 0x2, 0x519, 0x518, 0x3, 
    0x2, 0x2, 0x2, 0x519, 0x51a, 0x3, 0x2, 0x2, 0x2, 0x51a, 0x51b, 0x3, 
    0x2, 0x2, 0x2, 0x51b, 0x51c, 0x7, 0xa, 0x2, 0x2, 0x51c, 0xb1, 0x3, 0x2, 
    0x2, 0x2, 0x51d, 0x51e, 0x7, 0x77, 0x2, 0x2, 0x51e, 0x51f, 0x7, 0x5c, 
    0x2, 0x2, 0x51f, 0x520, 0x7, 0x77, 0x2, 0x2, 0x520, 0x528, 0x7, 0x47, 
    0x2, 0x2, 0x521, 0x522, 0x7, 0x77, 0x2, 0x2, 0x522, 0x523, 0x7, 0x5d, 
    0x2, 0x2, 0x523, 0x524, 0x7, 0x77, 0x2, 0x2, 0x524, 0x528, 0x7, 0x47, 
    0x2, 0x2, 0x525, 0x526, 0x7, 0x77, 0x2, 0x2, 0x526, 0x528, 0x7, 0x5e, 
    0x2, 0x2, 0x527, 0x51d, 0x3, 0x2, 0x2, 0x2, 0x527, 0x521, 0x3, 0x2, 
    0x2, 0x2, 0x527, 0x525, 0x3, 0x2, 0x2, 0x2, 0x528, 0x52a, 0x3, 0x2, 
    0x2, 0x2, 0x529, 0x52b, 0x7, 0x77, 0x2, 0x2, 0x52a, 0x529, 0x3, 0x2, 
    0x2, 0x2, 0x52a, 0x52b, 0x3, 0x2, 0x2, 0x2, 0x52b, 0x52c, 0x3, 0x2, 
    0x2, 0x2, 0x52c, 0x52d, 0x5, 0xb6, 0x5c, 0x2, 0x52d, 0xb3, 0x3, 0x2, 
    0x2, 0x2, 0x52e, 0x52f, 0x7, 0x77, 0x2, 0x2, 0x52f, 0x530, 0x7, 0x5f, 
    0x2, 0x2, 0x530, 0x531, 0x7, 0x77, 0x2, 0x2, 0x531, 0x539, 0x7, 0x60, 
    0x2, 0x2, 0x532, 0x533, 0x7, 0x77, 0x2, 0x2, 0x533, 0x534, 0x7, 0x5f, 
    0x2, 0x2, 0x534, 0x535, 0x7, 0x77, 0x2, 0x2, 0x535, 0x536, 0x7, 0x58, 
    0x2, 0x2, 0x536, 0x537, 0x7, 0x77, 0x2, 0x2, 0x537, 0x539, 0x7, 0x60, 
    0x2, 0x2, 0x538, 0x52e, 0x3, 0x2, 0x2, 0x2, 0x538, 0x532, 0x3, 0x2, 
    0x2, 0x2, 0x539, 0xb5, 0x3, 0x2, 0x2, 0x2, 0x53a, 0x53f, 0x5, 0xb8, 
    0x5d, 0x2, 0x53b, 0x53d, 0x7, 0x77, 0x2, 0x2, 0x53c, 0x53b, 0x3, 0x2, 
    0x2, 0x2, 0x53c, 0x53d, 0x3, 0x2, 0x2, 0x2, 0x53d, 0x53e, 0x3, 0x2, 
    0x2, 0x2, 0x53e, 0x540, 0x5, 0xc8, 0x65, 0x2, 0x53f, 0x53c, 0x3, 0x2, 
    0x2, 0x2, 0x53f, 0x540, 0x3, 0x2, 0x2, 0x2, 0x540, 0xb7, 0x3, 0x2, 0x2, 
    0x2, 0x541, 0x549, 0x5, 0xba, 0x5e, 0x2, 0x542, 0x549, 0x5, 0xd2, 0x6a, 
    0x2, 0x543, 0x549, 0x5, 0xca, 0x66, 0x2, 0x544, 0x549, 0x5, 0xc0, 0x61, 
    0x2, 0x545, 0x549, 0x5, 0xc2, 0x62, 0x2, 0x546, 0x549, 0x5, 0xc6, 0x64, 
    0x2, 0x547, 0x549, 0x5, 0xce, 0x68, 0x2, 0x548, 0x541, 0x3, 0x2, 0x2, 
    0x2, 0x548, 0x542, 0x3, 0x2, 0x2, 0x2, 0x548, 0x543, 0x3, 0x2, 0x2, 
    0x2, 0x548, 0x544, 0x3, 0x2, 0x2, 0x2, 0x548, 0x545, 0x3, 0x2, 0x2, 
    0x2, 0x548, 0x546, 0x3, 0x2, 0x2, 0x2, 0x548, 0x547, 0x3, 0x2, 0x2, 
    0x2, 0x549, 0xb9, 0x3, 0x2, 0x2, 0x2, 0x54a, 0x550, 0x5, 0xd0, 0x69, 
    0x2, 0x54b, 0x550, 0x7, 0x69, 0x2, 0x2, 0x54c, 0x550, 0x5, 0xbc, 0x5f, 
    0x2, 0x54d, 0x550, 0x7, 0x60, 0x2, 0x2, 0x54e, 0x550, 0x5, 0xbe, 0x60, 
    0x2, 0x54f, 0x54a, 0x3, 0x2, 0x2, 0x2, 0x54f, 0x54b, 0x3, 0x2, 0x2, 
    0x2, 0x54f, 0x54c, 0x3, 0x2, 0x2, 0x2, 0x54f, 0x54d, 0x3, 0x2, 0x2, 
    0x2, 0x54f, 0x54e, 0x3, 0x2, 0x2, 0x2, 0x550, 0xbb, 0x3, 0x2, 0x2, 0x2, 
    0x551, 0x552, 0x9, 0x7, 0x2, 0x2, 0x552, 0xbd, 0x3, 0x2, 0x2, 0x2, 0x553, 
    0x555, 0x7, 0x9, 0x2, 0x2, 0x554, 0x556, 0x7, 0x77, 0x2, 0x2, 0x555, 
    0x554, 0x3, 0x2, 0x2, 0x2, 0x555, 0x556, 0x3, 0x2, 0x2, 0x2, 0x556, 
    0x568, 0x3, 0x2, 0x2, 0x2, 0x557, 0x559, 0x5, 0x88, 0x45, 0x2, 0x558, 
    0x55a, 0x7, 0x77, 0x2, 0x2, 0x559, 0x558, 0x3, 0x2, 0x2, 0x2, 0x559, 
    0x55a, 0x3, 0x2, 0x2, 0x2, 0x55a, 0x565, 0x3, 0x2, 0x2, 0x2, 0x55b, 
    0x55d, 0x7, 0x6, 0x2, 0x2, 0x55c, 0x55e, 0x7, 0x77, 0x2, 0x2, 0x55d, 
    0x55c, 0x3, 0x2, 0x2, 0x2, 0x55d, 0x55e, 0x3, 0x2, 0x2, 0x2, 0x55e, 
    0x55f, 0x3, 0x2, 0x2, 0x2, 0x55f, 0x561, 0x5, 0x88, 0x45, 0x2, 0x560, 
    0x562, 0x7, 0x77, 0x2, 0x2, 0x561, 0x560, 0x3, 0x2, 0x2, 0x2, 0x561, 
    0x562, 0x3, 0x2, 0x2, 0x2, 0x562, 0x564, 0x3, 0x2, 0x2, 0x2, 0x563, 
    0x55b, 0x3, 0x2, 0x2, 0x2, 0x564, 0x567, 0x3, 0x2, 0x2, 0x2, 0x565, 
    0x563, 0x3, 0x2, 0x2, 0x2, 0x565, 0x566, 0x3, 0x2, 0x2, 0x2, 0x566, 
    0x569, 0x3, 0x2, 0x2, 0x2, 0x567, 0x565, 0x3, 0x2, 0x2, 0x2, 0x568, 
    0x557, 0x3, 0x2, 0x2, 0x2, 0x568, 0x569, 0x3, 0x2, 0x2, 0x2, 0x569, 
    0x56a, 0x3, 0x2, 0x2, 0x2, 0x56a, 0x56b, 0x7, 0xa, 0x2, 0x2, 0x56b, 
    0xbf, 0x3, 0x2, 0x2, 0x2, 0x56c, 0x56e, 0x7, 0x4, 0x2, 0x2, 0x56d, 0x56f, 
    0x7, 0x77, 0x2, 0x2, 0x56e, 0x56d, 0x3, 0x2, 0x2, 0x2, 0x56e, 0x56f, 
    0x3, 0x2, 0x2, 0x2, 0x56f, 0x570, 0x3, 0x2, 0x2, 0x2, 0x570, 0x572, 
    0x5, 0x88, 0x45, 0x2, 0x571, 0x573, 0x7, 0x77, 0x2, 0x2, 0x572, 0x571, 
    0x3, 0x2, 0x2, 0x2, 0x572, 0x573, 0x3, 0x2, 0x2, 0x2, 0x573, 0x574, 
    0x3, 0x2, 0x2, 0x2, 0x574, 0x575, 0x7, 0x5, 0x2, 0x2, 0x575, 0xc1, 0x3, 
    0x2, 0x2, 0x2, 0x576, 0x578, 0x5, 0xc4, 0x63, 0x2, 0x577, 0x579, 0x7, 
    0x77, 0x2, 0x2, 0x578, 0x577, 0x3, 0x2, 0x2, 0x2, 0x578, 0x579, 0x3, 
    0x2, 0x2, 0x2, 0x579, 0x57a, 0x3, 0x2, 0x2, 0x2, 0x57a, 0x57c, 0x7, 
    0x4, 0x2, 0x2, 0x57b, 0x57d, 0x7, 0x77, 0x2, 0x2, 0x57c, 0x57b, 0x3, 
    0x2, 0x2, 0x2, 0x57c, 0x57d, 0x3, 0x2, 0x2, 0x2, 0x57d, 0x57e, 0x3, 
    0x2, 0x2, 0x2, 0x57e, 0x580, 0x7, 0x4a, 0x2, 0x2, 0x57f, 0x581, 0x7, 
    0x77, 0x2, 0x2, 0x580, 0x57f, 0x3, 0x2, 0x2, 0x2, 0x580, 0x581, 0x3, 
    0x2, 0x2, 0x2, 0x581, 0x582, 0x3, 0x2, 0x2, 0x2, 0x582, 0x583, 0x7, 
    0x5, 0x2, 0x2, 0x583, 0x5a8, 0x3, 0x2, 0x2, 0x2, 0x584, 0x586, 0x5, 
    0xc4, 0x63, 0x2, 0x585, 0x587, 0x7, 0x77, 0x2, 0x2, 0x586, 0x585, 0x3, 
    0x2, 0x2, 0x2, 0x586, 0x587, 0x3, 0x2, 0x2, 0x2, 0x587, 0x588, 0x3, 
    0x2, 0x2, 0x2, 0x588, 0x58a, 0x7, 0x4, 0x2, 0x2, 0x589, 0x58b, 0x7, 
    0x77, 0x2, 0x2, 0x58a, 0x589, 0x3, 0x2, 0x2, 0x2, 0x58a, 0x58b, 0x3, 
    0x2, 0x2, 0x2, 0x58b, 0x590, 0x3, 0x2, 0x2, 0x2, 0x58c, 0x58e, 0x7, 
    0x49, 0x2, 0x2, 0x58d, 0x58f, 0x7, 0x77, 0x2, 0x2, 0x58e, 0x58d, 0x3, 
    0x2, 0x2, 0x2, 0x58e, 0x58f, 0x3, 0x2, 0x2, 0x2, 0x58f, 0x591, 0x3, 
    0x2, 0x2, 0x2, 0x590, 0x58c, 0x3, 0x2, 0x2, 0x2, 0x590, 0x591, 0x3, 
    0x2, 0x2, 0x2, 0x591, 0x5a3, 0x3, 0x2, 0x2, 0x2, 0x592, 0x594, 0x5, 
    0x88, 0x45, 0x2, 0x593, 0x595, 0x7, 0x77, 0x2, 0x2, 0x594, 0x593, 0x3, 
    0x2, 0x2, 0x2, 0x594, 0x595, 0x3, 0x2, 0x2, 0x2, 0x595, 0x5a0, 0x3, 
    0x2, 0x2, 0x2, 0x596, 0x598, 0x7, 0x6, 0x2, 0x2, 0x597, 0x599, 0x7, 
    0x77, 0x2, 0x2, 0x598, 0x597, 0x3, 0x2, 0x2, 0x2, 0x598, 0x599, 0x3, 
    0x2, 0x2, 0x2, 0x599, 0x59a, 0x3, 0x2, 0x2, 0x2, 0x59a, 0x59c, 0x5, 
    0x88, 0x45, 0x2, 0x59b, 0x59d, 0x7, 0x77, 0x2, 0x2, 0x59c, 0x59b, 0x3, 
    0x2, 0x2, 0x2, 0x59c, 0x59d, 0x3, 0x2, 0x2, 0x2, 0x59d, 0x59f, 0x3, 
    0x2, 0x2, 0x2, 0x59e, 0x596, 0x3, 0x2, 0x2, 0x2, 0x59f, 0x5a2, 0x3, 
    0x2, 0x2, 0x2, 0x5a0, 0x59e, 0x3, 0x2, 0x2, 0x2, 0x5a0, 0x5a1, 0x3, 
    0x2, 0x2, 0x2, 0x5a1, 0x5a4, 0x3, 0x2, 0x2, 0x2, 0x5a2, 0x5a0, 0x3, 
    0x2, 0x2, 0x2, 0x5a3, 0x592, 0x3, 0x2, 0x2, 0x2, 0x5a3, 0x5a4, 0x3, 
    0x2, 0x2, 0x2, 0x5a4, 0x5a5, 0x3, 0x2, 0x2, 0x2, 0x5a5, 0x5a6, 0x7, 
    0x5, 0x2, 0x2, 0x5a6, 0x5a8, 0x3, 0x2, 0x2, 0x2, 0x5a7, 0x576, 0x3, 
    0x2, 0x2, 0x2, 0x5a7, 0x584, 0x3, 0x2, 0x2, 0x2, 0x5a8, 0xc3, 0x3, 0x2, 
    0x2, 0x2, 0x5a9, 0x5aa, 0x5, 0xde, 0x70, 0x2, 0x5aa, 0xc5, 0x3, 0x2, 
    0x2, 0x2, 0x5ab, 0x5ad, 0x7, 0x63, 0x2, 0x2, 0x5ac, 0x5ae, 0x7, 0x77, 
    0x2, 0x2, 0x5ad, 0x5ac, 0x3, 0x2, 0x2, 0x2, 0x5ad, 0x5ae, 0x3, 0x2, 
    0x2, 0x2, 0x5ae, 0x5af, 0x3, 0x2, 0x2, 0x2, 0x5af, 0x5b1, 0x7, 0xb, 
    0x2, 0x2, 0x5b0, 0x5b2, 0x7, 0x77, 0x2, 0x2, 0x5b1, 0x5b0, 0x3, 0x2, 
    0x2, 0x2, 0x5b1, 0x5b2, 0x3, 0x2, 0x2, 0x2, 0x5b2, 0x5b3, 0x3, 0x2, 
    0x2, 0x2, 0x5b3, 0x5b5, 0x7, 0x42, 0x2, 0x2, 0x5b4, 0x5b6, 0x7, 0x77, 
    0x2, 0x2, 0x5b5, 0x5b4, 0x3, 0x2, 0x2, 0x2, 0x5b5, 0x5b6, 0x3, 0x2, 
    0x2, 0x2, 0x5b6, 0x5b7, 0x3, 0x2, 0x2, 0x2, 0x5b7, 0x5bc, 0x5, 0x6a, 
    0x36, 0x2, 0x5b8, 0x5ba, 0x7, 0x77, 0x2, 0x2, 0x5b9, 0x5b8, 0x3, 0x2, 
    0x2, 0x2, 0x5b9, 0x5ba, 0x3, 0x2, 0x2, 0x2, 0x5ba, 0x5bb, 0x3, 0x2, 
    0x2, 0x2, 0x5bb, 0x5bd, 0x5, 0x68, 0x35, 0x2, 0x5bc, 0x5b9, 0x3, 0x2, 
    0x2, 0x2, 0x5bc, 0x5bd, 0x3, 0x2, 0x2, 0x2, 0x5bd, 0x5bf, 0x3, 0x2, 
    0x2, 0x2, 0x5be, 0x5c0, 0x7, 0x77, 0x2, 0x2, 0x5bf, 0x5be, 0x3, 0x2, 
    0x2, 0x2, 0x5bf, 0x5c0, 0x3, 0x2, 0x2, 0x2, 0x5c0, 0x5c1, 0x3, 0x2, 
    0x2, 0x2, 0x5c1, 0x5c2, 0x7, 0xd, 0x2, 0x2, 0x5c2, 0xc7, 0x3, 0x2, 0x2, 
    0x2, 0x5c3, 0x5c5, 0x7, 0x1b, 0x2, 0x2, 0x5c4, 0x5c6, 0x7, 0x77, 0x2, 
    0x2, 0x5c5, 0x5c4, 0x3, 0x2, 0x2, 0x2, 0x5c5, 0x5c6, 0x3, 0x2, 0x2, 
    0x2, 0x5c6, 0x5c7, 0x3, 0x2, 0x2, 0x2, 0x5c7, 0x5c8, 0x5, 0xd6, 0x6c, 
    0x2, 0x5c8, 0xc9, 0x3, 0x2, 0x2, 0x2, 0x5c9, 0x5ce, 0x7, 0x64, 0x2, 
    0x2, 0x5ca, 0x5cc, 0x7, 0x77, 0x2, 0x2, 0x5cb, 0x5ca, 0x3, 0x2, 0x2, 
    0x2, 0x5cb, 0x5cc, 0x3, 0x2, 0x2, 0x2, 0x5cc, 0x5cd, 0x3, 0x2, 0x2, 
    0x2, 0x5cd, 0x5cf, 0x5, 0xcc, 0x67, 0x2, 0x5ce, 0x5cb, 0x3, 0x2, 0x2, 
    0x2, 0x5cf, 0x5d0, 0x3, 0x2, 0x2, 0x2, 0x5d0, 0x5ce, 0x3, 0x2, 0x2, 
    0x2, 0x5d0, 0x5d1, 0x3, 0x2, 0x2, 0x2, 0x5d1, 0x5e0, 0x3, 0x2, 0x2, 
    0x2, 0x5d2, 0x5d4, 0x7, 0x64, 0x2, 0x2, 0x5d3, 0x5d5, 0x7, 0x77, 0x2, 
    0x2, 0x5d4, 0x5d3, 0x3, 0x2, 0x2, 0x2, 0x5d4, 0x5d5, 0x3, 0x2, 0x2, 
    0x2, 0x5d5, 0x5d6, 0x3, 0x2, 0x2, 0x2, 0x5d6, 0x5db, 0x5, 0x88, 0x45, 
    0x2, 0x5d7, 0x5d9, 0x7, 0x77, 0x2, 0x2, 0x5d8, 0x5d7, 0x3, 0x2, 0x2, 
    0x2, 0x5d8, 0x5d9, 0x3, 0x2, 0x2, 0x2, 0x5d9, 0x5da, 0x3, 0x2, 0x2, 
    0x2, 0x5da, 0x5dc, 0x5, 0xcc, 0x67, 0x2, 0x5db, 0x5d8, 0x3, 0x2, 0x2, 
    0x2, 0x5dc, 0x5dd, 0x3, 0x2, 0x2, 0x2, 0x5dd, 0x5db, 0x3, 0x2, 0x2, 
    0x2, 0x5dd, 0x5de, 0x3, 0x2, 0x2, 0x2, 0x5de, 0x5e0, 0x3, 0x2, 0x2, 
    0x2, 0x5df, 0x5c9, 0x3, 0x2, 0x2, 0x2, 0x5df, 0x5d2, 0x3, 0x2, 0x2, 
    0x2, 0x5e0, 0x5e9, 0x3, 0x2, 0x2, 0x2, 0x5e1, 0x5e3, 0x7, 0x77, 0x2, 
    0x2, 0x5e2, 0x5e1, 0x3, 0x2, 0x2, 0x2, 0x5e2, 0x5e3, 0x3, 0x2, 0x2, 
    0x2, 0x5e3, 0x5e4, 0x3, 0x2, 0x2, 0x2, 0x5e4, 0x5e6, 0x7, 0x65, 0x2, 
    0x2, 0x5e5, 0x5e7, 0x7, 0x77, 0x2, 0x2, 0x5e6, 0x5e5, 0x3, 0x2, 0x2, 
    0x2, 0x5e6, 0x5e7, 0x3, 0x2, 0x2, 0x2, 0x5e7, 0x5e8, 0x3, 0x2, 0x2, 
    0x2, 0x5e8, 0x5ea, 0x5, 0x88, 0x45, 0x2, 0x5e9, 0x5e2, 0x3, 0x2, 0x2, 
    0x2, 0x5e9, 0x5ea, 0x3, 0x2, 0x2, 0x2, 0x5ea, 0x5ec, 0x3, 0x2, 0x2, 
    0x2, 0x5eb, 0x5ed, 0x7, 0x77, 0x2, 0x2, 0x5ec, 0x5eb, 0x3, 0x2, 0x2, 
    0x2, 0x5ec, 0x5ed, 0x3, 0x2, 0x2, 0x2, 0x5ed, 0x5ee, 0x3, 0x2, 0x2, 
    0x2, 0x5ee, 0x5ef, 0x7, 0x66, 0x2, 0x2, 0x5ef, 0xcb, 0x3, 0x2, 0x2, 
    0x2, 0x5f0, 0x5f2, 0x7, 0x67, 0x2, 0x2, 0x5f1, 0x5f3, 0x7, 0x77, 0x2, 
    0x2, 0x5f2, 0x5f1, 0x3, 0x2, 0x2, 0x2, 0x5f2, 0x5f3, 0x3, 0x2, 0x2, 
    0x2, 0x5f3, 0x5f4, 0x3, 0x2, 0x2, 0x2, 0x5f4, 0x5f6, 0x5, 0x88, 0x45, 
    0x2, 0x5f5, 0x5f7, 0x7, 0x77, 0x2, 0x2, 0x5f6, 0x5f5, 0x3, 0x2, 0x2, 
    0x2, 0x5f6, 0x5f7, 0x3, 0x2, 0x2, 0x2, 0x5f7, 0x5f8, 0x3, 0x2, 0x2, 
    0x2, 0x5f8, 0x5fa, 0x7, 0x68, 0x2, 0x2, 0x5f9, 0x5fb, 0x7, 0x77, 0x2, 
    0x2, 0x5fa, 0x5f9, 0x3, 0x2, 0x2, 0x2, 0x5fa, 0x5fb, 0x3, 0x2, 0x2, 
    0x2, 0x5fb, 0x5fc, 0x3, 0x2, 0x2, 0x2, 0x5fc, 0x5fd, 0x5, 0x88, 0x45, 
    0x2, 0x5fd, 0xcd, 0x3, 0x2, 0x2, 0x2, 0x5fe, 0x5ff, 0x5, 0xde, 0x70, 
    0x2, 0x5ff, 0xcf, 0x3, 0x2, 0x2, 0x2, 0x600, 0x603, 0x5, 0xda, 0x6e, 
    0x2, 0x601, 0x603, 0x5, 0xd8, 0x6d, 0x2, 0x602, 0x600, 0x3, 0x2, 0x2, 
    0x2, 0x602, 0x601, 0x3, 0x2, 0x2, 0x2, 0x603, 0xd1, 0x3, 0x2, 0x2, 0x2, 
    0x604, 0x607, 0x7, 0x1c, 0x2, 0x2, 0x605, 0x608, 0x5, 0xde, 0x70, 0x2, 
    0x606, 0x608, 0x7, 0x6b, 0x2, 0x2, 0x607, 0x605, 0x3, 0x2, 0x2, 0x2, 
    0x607, 0x606, 0x3, 0x2, 0x2, 0x2, 0x608, 0xd3, 0x3, 0x2, 0x2, 0x2, 0x609, 
    0x60b, 0x5, 0xb8, 0x5d, 0x2, 0x60a, 0x60c, 0x7, 0x77, 0x2, 0x2, 0x60b, 
    0x60a, 0x3, 0x2, 0x2, 0x2, 0x60b, 0x60c, 0x3, 0x2, 0x2, 0x2, 0x60c, 
    0x60d, 0x3, 0x2, 0x2, 0x2, 0x60d, 0x60e, 0x5, 0xc8, 0x65, 0x2, 0x60e, 
    0xd5, 0x3, 0x2, 0x2, 0x2, 0x60f, 0x610, 0x5, 0xdc, 0x6f, 0x2, 0x610, 
    0xd7, 0x3, 0x2, 0x2, 0x2, 0x611, 0x612, 0x7, 0x6b, 0x2, 0x2, 0x612, 
    0xd9, 0x3, 0x2, 0x2, 0x2, 0x613, 0x614, 0x7, 0x72, 0x2, 0x2, 0x614, 
    0xdb, 0x3, 0x2, 0x2, 0x2, 0x615, 0x616, 0x5, 0xde, 0x70, 0x2, 0x616, 
    0xdd, 0x3, 0x2, 0x2, 0x2, 0x617, 0x61c, 0x7, 0x73, 0x2, 0x2, 0x618, 
    0x619, 0x7, 0x76, 0x2, 0x2, 0x619, 0x61c, 0x8, 0x70, 0x1, 0x2, 0x61a, 
    0x61c, 0x7, 0x6c, 0x2, 0x2, 0x61b, 0x617, 0x3, 0x2, 0x2, 0x2, 0x61b, 
    0x618, 0x3, 0x2, 0x2, 0x2, 0x61b, 0x61a, 0x3, 0x2, 0x2, 0x2, 0x61c, 
    0xdf, 0x3, 0x2, 0x2, 0x2, 0x61d, 0x61e, 0x9, 0x8, 0x2, 0x2, 0x61e, 0xe1, 
    0x3, 0x2, 0x2, 0x2, 0x61f, 0x620, 0x9, 0x9, 0x2, 0x2, 0x620, 0xe3, 0x3, 
    0x2, 0x2, 0x2, 0x621, 0x622, 0x9, 0xa, 0x2, 0x2, 0x622, 0xe5, 0x3, 0x2, 
    0x2, 0x2, 0x10f, 0xe7, 0xea, 0xed, 0xf2, 0xf5, 0xf8, 0xfb, 0x107, 0x10b, 
    0x10f, 0x113, 0x117, 0x11b, 0x120, 0x125, 0x129, 0x131, 0x13b, 0x13f, 
    0x143, 0x147, 0x14c, 0x158, 0x15c, 0x160, 0x164, 0x168, 0x16a, 0x16e, 
    0x172, 0x174, 0x18a, 0x195, 0x1ab, 0x1af, 0x1b4, 0x1c1, 0x1c5, 0x1ca, 
    0x1cf, 0x1d3, 0x1d8, 0x1e3, 0x1e7, 0x1eb, 0x1f3, 0x1f9, 0x201, 0x20d, 
    0x212, 0x217, 0x21b, 0x220, 0x226, 0x22b, 0x22e, 0x232, 0x236, 0x23a, 
    0x240, 0x244, 0x249, 0x24e, 0x252, 0x255, 0x259, 0x25d, 0x261, 0x265, 
    0x269, 0x26f, 0x273, 0x278, 0x27c, 0x284, 0x288, 0x28c, 0x290, 0x294, 
    0x297, 0x29b, 0x2a5, 0x2ab, 0x2af, 0x2b3, 0x2b8, 0x2bd, 0x2c1, 0x2c7, 
    0x2cb, 0x2cf, 0x2d4, 0x2da, 0x2dd, 0x2e3, 0x2e6, 0x2ec, 0x2f0, 0x2f4, 
    0x2f8, 0x2fc, 0x301, 0x306, 0x30a, 0x30f, 0x312, 0x31b, 0x324, 0x329, 
    0x336, 0x339, 0x341, 0x345, 0x34a, 0x353, 0x358, 0x35f, 0x363, 0x367, 
    0x369, 0x36d, 0x36f, 0x373, 0x375, 0x379, 0x37d, 0x37f, 0x383, 0x385, 
    0x389, 0x38b, 0x38e, 0x392, 0x398, 0x39c, 0x39f, 0x3a2, 0x3a8, 0x3ab, 
    0x3ae, 0x3b2, 0x3b6, 0x3ba, 0x3be, 0x3c0, 0x3c4, 0x3c6, 0x3ca, 0x3cc, 
    0x3d0, 0x3d2, 0x3d8, 0x3dc, 0x3e0, 0x3e4, 0x3e8, 0x3ec, 0x3f0, 0x3f4, 
    0x3f8, 0x3fb, 0x401, 0x405, 0x409, 0x40c, 0x411, 0x416, 0x41b, 0x420, 
    0x426, 0x42a, 0x42e, 0x43f, 0x449, 0x453, 0x458, 0x45a, 0x460, 0x464, 
    0x468, 0x46c, 0x470, 0x478, 0x47c, 0x480, 0x484, 0x48a, 0x48e, 0x494, 
    0x498, 0x49d, 0x4a2, 0x4a6, 0x4ab, 0x4b0, 0x4b4, 0x4ba, 0x4c1, 0x4c5, 
    0x4cb, 0x4d2, 0x4d6, 0x4dc, 0x4e3, 0x4e7, 0x4ec, 0x4f1, 0x4f3, 0x4f7, 
    0x4fa, 0x500, 0x504, 0x507, 0x50a, 0x511, 0x515, 0x519, 0x527, 0x52a, 
    0x538, 0x53c, 0x53f, 0x548, 0x54f, 0x555, 0x559, 0x55d, 0x561, 0x565, 
    0x568, 0x56e, 0x572, 0x578, 0x57c, 0x580, 0x586, 0x58a, 0x58e, 0x590, 
    0x594, 0x598, 0x59c, 0x5a0, 0x5a3, 0x5a7, 0x5ad, 0x5b1, 0x5b5, 0x5b9, 
    0x5bc, 0x5bf, 0x5c5, 0x5cb, 0x5d0, 0x5d4, 0x5d8, 0x5dd, 0x5df, 0x5e2, 
    0x5e6, 0x5e9, 0x5ec, 0x5f2, 0x5f6, 0x5fa, 0x602, 0x607, 0x60b, 0x61b, 
  };

  atn::ATNDeserializer deserializer;
  _atn = deserializer.deserialize(_serializedATN);

  size_t count = _atn.getNumberOfDecisions();
  _decisionToDFA.reserve(count);
  for (size_t i = 0; i < count; i++) { 
    _decisionToDFA.emplace_back(_atn.getDecisionState(i), i);
  }
}

CypherParser::Initializer CypherParser::_init;
