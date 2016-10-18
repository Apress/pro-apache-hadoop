REGISTER prohadoop-0.0.1-SNAPSHOT.jar;
DEFINE MY_IF org.apress.prohadoop.c11.CustomIf;

A = LOAD 'input/pigsample/a.txt' USING PigStorage(',') AS (a:int,b:chararray,c:int);
B = LOAD 'input/pigsample/b.txt' USING PigStorage(',') AS (d:int,e:chararray,f:int);
C = MY_IF(A);
D = JOIN C by a,B by f;
E = GROUP D by b;
F = FOREACH E GENERATE group, COUNT(D);
STORE F INTO 'output/pigcustomif/';
