COPY `person` (ID,fName,gender,isStudent,isWorker,age,eyeSight,birthdate,registerTime,lastJobDuration,workedHours,usedNames,courseScoresPerTerm,grades,height,u) FROM "person.csv" (header=true);
COPY `organisation` (ID,name,orgCode,mark,score,history,licenseValidInterval,rating,state,info) FROM "organisation.csv" (header=true);
COPY `movies` (name,length,note,description,content,audience,grade) FROM "movies.csv" (header=true);
COPY `studyAt` (year,places,length,level,code,temperature,ulength,ulevel,hugedata) FROM "studyAt.csv" (header=true);
COPY `workAt` (year,grading,rating) FROM "workAt.csv" (header=true);
COPY `meets` (location,times,data) FROM "meets.csv" (header=true);
COPY `knows` (date,meetTime,validInterval,comments,summary,notes,someMap) FROM "knows.csv" (header=true);
COPY `marries` (usedAddress,address,note) FROM "marries.csv" (header=true);
