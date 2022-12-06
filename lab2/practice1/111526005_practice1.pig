records  = load '/user/s111526005/lab2_practice1/pdata.txt' using PigStorage(' ') as (year:int, temperature:int, quality:int);

rec1 = filter records by year > 1951 and (quality == 1);

rec2 = group rec1 by temperature;

maxt = FOREACH rec2 GENERATE group, MAX(rec1.year);

dump maxt;
