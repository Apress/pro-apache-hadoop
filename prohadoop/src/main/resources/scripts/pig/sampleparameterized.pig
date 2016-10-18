/*pig -x local -param A_LOC=input/pigsample/a.txt -param B_LOC=input/pigsample/b.txt -param OUTPUT_LOC=sampleout2 sampleparameterized.pig  */
A = LOAD '$A_LOC' USING PigStorage(',') AS (a:int,b:chararray,c:int);
B = LOAD '$B_LOC' USING PigStorage(',') AS (d:int,e:chararray,f:int);
C = FILTER A BY (b=='A') or (b=='B');
D = JOIN C by a,B by f;
E = GROUP D by b;
F = FOREACH E GENERATE group, COUNT(D);
STORE F INTO '$OUTPUT_LOC';
