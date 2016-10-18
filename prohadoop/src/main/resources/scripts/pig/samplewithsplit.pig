X = LOAD 'input/pigsample/a.txt' USING PigStorage(',') AS (a:int,b:chararray,c:int);
SPLIT X into A if b=='A',B if b=='B',C if b=='C',D otherwise;
STORE A INTO 'output/A/';
STORE B INTO 'output/B/';
STORE C INTO 'output/C/';
STORE D INTO 'output/D/';
