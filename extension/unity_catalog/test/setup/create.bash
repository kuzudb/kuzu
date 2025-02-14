${1}/bin/uc table delete --full_name university.default.grades
${1}/bin/uc table delete --full_name university.default.person
${1}/bin/uc catalog create --name university
${1}/bin/uc schema create --catalog university --name default
${1}/bin/uc table create --full_name university.default.grades --columns "name string, pass boolean, math short, chinese int, english long, physics float, chemistry double" --format DELTA --storage_location "${2}/extension/unity_catalog/test/setup/tables/grades"
${1}/bin/uc table create --full_name university.default.person --columns "name string, class byte, birthtime timestamp, info binary" --format DELTA --storage_location "${2}/extension/unity_catalog/test/setup/tables/person"
