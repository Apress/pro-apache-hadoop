A = LOAD 'input/pigsample/a.txt' USING PigStorage(',') AS (a:int,b:chararray,c:int);
B = LOAD 'input/pigsample/a.txt' USING PigStorage(',') AS (a:int,b:chararray,c:int);
C = FILTER A BY (b=='A') or (b=='B');
D = FILTER B BY (b=='B');
E = UNION C,D;
F = DISTINCT E;
DUMP D;