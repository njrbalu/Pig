REGISTER 'RateChanges.jar';
A = LOAD 'NYSE_20141224.txt' using PigStorage(',') AS (symbol:chararray,date:long,open:float,high:float,low:float,close:float,volume:long);
B = GROUP A BY date;
C = FOREACH B GENERATE group,(group IS NOT NULL? RateChanges(A) : ());
DUMP C;