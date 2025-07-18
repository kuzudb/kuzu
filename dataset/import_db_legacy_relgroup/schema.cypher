CREATE NODE TABLE `organisation` (`id` SERIAL, PRIMARY KEY(`id`));
CREATE NODE TABLE `person` (`id` SERIAL, PRIMARY KEY(`id`));
CREATE REL TABLE `knows` (FROM `person` TO `person`, FROM `person` TO `organisation`);
DROP SEQUENCE IF EXISTS `organisation_id_serial`;
CREATE SEQUENCE IF NOT EXISTS `organisation_id_serial` START 2 INCREMENT 1 MINVALUE 0 MAXVALUE 9223372036854775807 NO CYCLE;
RETURN nextval('organisation_id_serial');
DROP SEQUENCE IF EXISTS `person_id_serial`;
CREATE SEQUENCE IF NOT EXISTS `person_id_serial` START 3 INCREMENT 1 MINVALUE 0 MAXVALUE 9223372036854775807 NO CYCLE;
RETURN nextval('person_id_serial');
