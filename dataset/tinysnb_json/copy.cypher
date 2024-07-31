load extension "extension/json/build/libjson.kuzu_extension";
copy person from "dataset/tinysnb_json/vPerson.json";
copy person from "dataset/tinysnb_json/vPerson2.json" (format="unstructured")
copy organisation from "dataset/tinysnb_json/vOrganisation.json";
copy movies from "dataset/tinysnb_json/vMovies.json";
copy knows from "dataset/tinysnb_json/eKnows.json";
copy knows from "dataset/tinysnb_json/eKnows_2.json";
copy studyAt from "dataset/tinysnb_json/eStudyAt.json";
copy workAt from "dataset/tinysnb_json/eWorkAt.json";
copy meets from "dataset/tinysnb_json/eMeets.json";
copy marries from "dataset/tinysnb_json/eMarries.json";
