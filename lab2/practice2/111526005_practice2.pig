records = load '/user/s111526005/lab2_practice2/pbad.txt' using PigStorage(' ') as (year:int, temperature:float, quality:chararray);

new_data = filter records by quality MATCHES '[0123456789]';

old_data = filter records by NOT quality MATCHES '[0123456789]';

convert_old_data = group old_data by year;

maxt = FOREACH convert_old_data GENERATE group, MAX(old_data.temperature) * 9/5 + 32;

dump maxt;
