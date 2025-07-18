CREATE NODE TABLE `nodes` (`id` INT64, PRIMARY KEY(`id`));
CREATE REL TABLE `edges` (FROM `nodes` TO `nodes`);
