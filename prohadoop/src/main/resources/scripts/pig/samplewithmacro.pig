DEFINE get_A_relation(criteria1) returns C {
   A = LOAD 'input/pigsample/a.txt' USING PigStorage(',') AS (a:int,b:chararray,c:int);
   $C = FILTER A BY (b=='$criteria1');
};

B = LOAD 'input/pigsample/b.txt' USING PigStorage(',') AS (d:int,e:chararray,f:int);
C = get_A_relation('A');
D = JOIN C by a,B by f;
E = GROUP D by b;
F = FOREACH E GENERATE group, COUNT(D);
STORE F INTO 'output/pigsample/';
