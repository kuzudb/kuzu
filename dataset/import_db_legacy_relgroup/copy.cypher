COPY `organisation` (`id`) FROM "organisation.parquet" ;
COPY `person` (`id`) FROM "person.parquet" ;
COPY `knows_person_person` FROM "knows_person_person.parquet" ;
COPY `knows_person_organisation` FROM "knows_person_organisation.parquet" ;
