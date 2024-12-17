--
-- PostgreSQL database dump
--

-- Dumped from database version 14.13 (Homebrew)
-- Dumped by pg_dump version 14.13 (Homebrew)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: kuzu; Type: SCHEMA; Schema: -; Owner: ci
--

CREATE SCHEMA kuzu;


ALTER SCHEMA kuzu OWNER TO ci;

--
-- Name: audience_type; Type: TYPE; Schema: public; Owner: ci
--

CREATE TYPE public.audience_type AS (
	key character varying,
	value bigint
);


ALTER TYPE public.audience_type OWNER TO ci;

--
-- Name: description_type; Type: TYPE; Schema: public; Owner: ci
--

CREATE TYPE public.description_type AS (
	rating double precision,
	stars bigint,
	views bigint,
	release timestamp without time zone,
	release_ns timestamp without time zone,
	release_ms timestamp without time zone,
	release_sec timestamp without time zone,
	release_tz timestamp with time zone,
	film date,
	u8 smallint,
	u16 smallint,
	u32 integer,
	u64 bigint,
	hugedata numeric
);


ALTER TYPE public.description_type OWNER TO ci;

--
-- Name: mood; Type: TYPE; Schema: public; Owner: ci
--

CREATE TYPE public.mood AS ENUM (
    'sad',
    'ok',
    'happy'
);


ALTER TYPE public.mood OWNER TO ci;

--
-- Name: state_type; Type: TYPE; Schema: public; Owner: ci
--

CREATE TYPE public.state_type AS (
	revenue smallint,
	location character varying[]
);


ALTER TYPE public.state_type OWNER TO ci;

--
-- Name: stock_type; Type: TYPE; Schema: public; Owner: ci
--

CREATE TYPE public.stock_type AS (
	price bigint[],
	volume bigint
);


ALTER TYPE public.stock_type OWNER TO ci;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: user; Type: TABLE; Schema: kuzu; Owner: ci
--

CREATE TABLE kuzu."user" (
    id integer,
    org character varying,
    rate integer
);


ALTER TABLE kuzu."user" OWNER TO ci;

--
-- Name: movies; Type: TABLE; Schema: public; Owner: ci
--

CREATE TABLE public.movies (
    name character varying NOT NULL,
    length integer,
    note character varying,
    description public.description_type,
    content bytea,
    audience public.audience_type[]
);


ALTER TABLE public.movies OWNER TO ci;

--
-- Name: organisation; Type: TABLE; Schema: public; Owner: ci
--

CREATE TABLE public.organisation (
    id bigint NOT NULL,
    name character varying,
    orgcode bigint,
    mark double precision,
    score bigint,
    history interval,
    licensevalidinterval interval,
    rating double precision,
    state public.state_type,
    stock public.stock_type,
    info character varying
);


ALTER TABLE public.organisation OWNER TO ci;

--
-- Name: person; Type: TABLE; Schema: public; Owner: ci
--

CREATE TABLE public.person (
    id bigint NOT NULL,
    fname character varying,
    gender bigint,
    isstudent boolean,
    isworker boolean,
    age bigint,
    eyesight double precision,
    birthdate date,
    registertime timestamp without time zone,
    lastjobduration interval,
    workedhours bigint[],
    usednames character varying[],
    height double precision,
    u uuid
);


ALTER TABLE public.person OWNER TO ci;

--
-- Name: persontest; Type: TABLE; Schema: public; Owner: ci
--

CREATE TABLE public.persontest (
    id integer
);


ALTER TABLE public.persontest OWNER TO ci;

--
-- Data for Name: user; Type: TABLE DATA; Schema: kuzu; Owner: ci
--

COPY kuzu."user" (id, org, rate) FROM stdin;
5	apple	4
7	ms	5
9	blackberry	7
\.


--
-- Data for Name: movies; Type: TABLE DATA; Schema: public; Owner: ci
--

COPY public.movies (name, length, note, description, content, audience) FROM stdin;
Sóló cón tu párejâ	126	this is a very very good movie	(5.3,2,152,"2011-08-20 11:25:30","2011-08-20 11:25:30","2011-08-20 11:25:30","2011-08-20 11:25:30","2011-08-20 11:25:30+08",2012-05-11,220,20,1,180,1844674407370955161811111111)	\\x5c7841415c784142696e746572657374696e675c783042	{"(audience1,52)","(audience53,42)"}
The 😂😃🧘🏻‍♂️🌍🌦️🍞🚗 movie	2544	the movie is very very good	(7,10,982,"2018-11-13 13:33:11","2018-11-13 13:33:11","2018-11-13 13:33:11","2018-11-13 13:33:11","2018-11-13 13:33:11+08",2014-09-12,12,120,55,1,-1844674407370955161511)	\\x5c7841425c784344	{"(audience1,33)"}
Roma	298	the movie is very interesting and funny	(1223,100,10003,"2011-02-11 16:44:22","2011-02-11 16:44:22","2011-02-11 16:44:22","2011-02-11 16:44:22","2011-02-11 16:44:22+08",2013-02-22,1,15,200,4,-15)	\\x707572652061736369692063686172616374657273	{}
\.


--
-- Data for Name: organisation; Type: TABLE DATA; Schema: public; Owner: ci
--

COPY public.organisation (id, name, orgcode, mark, score, history, licensevalidinterval, rating, state, stock, info) FROM stdin;
1	ABFsUni	325	3.7	-2	10 years 5 mons 13:00:00.000024	3 years 5 days	1	(138,"{toronto,""montr,eal""}")	("{96,56}",1000)	3.12
4	CsWork	934	4.1	-100	2 years 4 days 10:00:00	26 years 52 days 48:00:00	0.78	(152,"{""vanco,uver north area""}")	("{15,78,671}",432)	abcd
6	DEsWork	824	4.1	7	2 years 04:34:00.000022	82:00:00.1	0.52	(558,"{""very long city name"",""new york""}")	({22},99)	2023-12-15
\.


--
-- Data for Name: person; Type: TABLE DATA; Schema: public; Owner: ci
--

COPY public.person (id, fname, gender, isstudent, isworker, age, eyesight, birthdate, registertime, lastjobduration, workedhours, usednames, height, u) FROM stdin;
0	Alice	1	t	f	35	5	1900-01-01	2011-08-20 11:25:30	3 years 2 days 13:02:00	{10,5}	{Aida}	1.731	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11
2	Bob	2	t	f	30	5.1	1900-01-01	2008-11-03 15:25:30.000526	10 years 5 mons 13:00:00.000024	{12,8}	{Bobby}	0.99	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a12
3	Carol	1	f	t	45	5	1940-06-22	1911-08-20 02:32:21	48:24:11	{4,5}	{Carmen,Fred}	1	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a13
5	Dan	2	f	t	20	4.8	1950-07-23	2031-11-30 12:25:30	10 years 5 mons 13:00:00.000024	{1,9}	{Wolfeschlegelstein,Daniel}	1.3	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a14
7	Elizabeth	1	f	t	20	4.7	1980-10-26	1976-12-23 11:21:42	48:24:11	{2}	{Ein}	1.463	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a15
8	Farooq	2	t	f	25	4.5	1980-10-26	1972-07-31 13:22:30.678559	00:18:00.024	{3,4,5,6,7}	{Fesdwe}	1.51	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a16
9	Greg	2	f	f	40	4.9	1980-10-26	1976-12-23 11:21:42	10 years 5 mons 13:00:00.000024	{1}	{Grad}	1.6	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a17
10	Hubert Blaine Wolfeschlegelsteinhausenbergerdorff	2	f	t	83	4.9	1990-11-27	2023-02-21 13:25:30	3 years 2 days 13:02:00	{10,11,12,3,4,5,6,7}	{Ad,De,Hi,Kye,Orlan}	1.323	a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a18
\.


--
-- Data for Name: persontest; Type: TABLE DATA; Schema: public; Owner: ci
--

COPY public.persontest (id) FROM stdin;
\.


--
-- Name: movies movies_pkey; Type: CONSTRAINT; Schema: public; Owner: ci
--

ALTER TABLE ONLY public.movies
    ADD CONSTRAINT movies_pkey PRIMARY KEY (name);


--
-- Name: organisation organisation_pkey; Type: CONSTRAINT; Schema: public; Owner: ci
--

ALTER TABLE ONLY public.organisation
    ADD CONSTRAINT organisation_pkey PRIMARY KEY (id);


--
-- Name: person person_pkey; Type: CONSTRAINT; Schema: public; Owner: ci
--

ALTER TABLE ONLY public.person
    ADD CONSTRAINT person_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

