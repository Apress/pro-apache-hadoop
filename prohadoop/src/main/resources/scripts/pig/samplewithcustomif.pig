A = LOAD 'input/pigsample/a.txt' USING PigStorage(',') AS (a:int,b:chararray,c:int);
B = LOAD 'input/pigsample/b.txt' USING PigStorage(',') AS (d:int,e:chararray,f:int);
C = FILTER A BY (b=='A') or (b=='B');
D = JOIN C by a,B by f;
E = GROUP D by b;
F = FOREACH E GENERATE group, COUNT(D);
STORE F INTO 'output/pigsample/';
