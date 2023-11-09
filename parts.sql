/*

  Код для работы с партициями postgresql

  Аналог из citus:
  SELECT create_time_partitions(table_name:= 'history',
    partition_interval:= '1 week',
    end_at:= '2023-11-16',
    start_from:= '2023-11-02');

  Аналог из timescaledb:
  SELECT create_hypertable('history', 'clock', chunk_time_interval => 86400, migrate_data => true);

*/

CREATE OR REPLACE PROCEDURE create_parts(
  table_name    TEXT
, time_interval INT  DEFAULT 604800     -- 7 days
, chunk_count   INT  DEFAULT 2
, time_min      INT  DEFAULT NULL
, child_prefix  TEXT DEFAULT NULL
)
LANGUAGE plpgsql AS $_$
/*
  Создание партиций для таблицы table_name
  * заданный момент времени time_min [now()] округляется до time_interval
  * результат - это суффикс имени партиции и ее начальный интервал
  * конечный интервал определяется добавлением time_interval
  * если партиции с таким именем нет, она создается
  * начальный интервал увеличивается на time_interval
  * повторить chunk_count раз
*/
DECLARE
  chunk_from INT;
  i INT;
  table_new TEXT;
BEGIN
  IF time_min IS NULL THEN
    -- если начальное время не задано, берем текущее
    time_min := extract(epoch from now());
  END IF;
  -- округляем до заданного шага
  chunk_from := time_interval * (time_min / time_interval)::INT;
  FOR i IN 1..chunk_count LOOP
    -- имя новой таблицы, префикс совпадает с текущей, если не задан явно
    table_new := format('%s_p%s', COALESCE(child_prefix, table_name), chunk_from);
    RAISE NOTICE '%: FROM % TO %', table_new, chunk_from, chunk_from + time_interval;
    if to_regclass(table_new) is null then
      -- создаем, если такого имени нет
      execute format('create table %I partition of %I for values from (%L) to (%L)'
              , table_new, table_name, chunk_from, chunk_from + time_interval);
    else
      raise notice '  already exists';
    end if;
    chunk_from := chunk_from + time_interval;
  END LOOP;
END
$_$;

CREATE OR REPLACE PROCEDURE create_parts_for_all(
  time_interval INT  DEFAULT 604800     -- 7 days
, chunk_count   INT  DEFAULT 2
, time_min      INT  DEFAULT NULL
, child_prefix  TEXT DEFAULT NULL
)
LANGUAGE plpgsql AS $_$
/*
  Создание партиций для всех таблиц с партициями
*/
DECLARE
  table_name TEXT;
BEGIN
  FOR table_name IN SELECT
    c.relname
    -- format('%I.%I', n.nspname, c.relname)
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on c.relnamespace = n.oid
    join pg_partitioned_table p on p.partrelid = c.oid
    order by n.nspname, c.relname
  LOOP
    call create_parts(table_name, time_interval, chunk_count, time_min);
  END LOOP;
END
$_$;

CREATE OR REPLACE PROCEDURE create_default_parts_for_all()
LANGUAGE plpgsql AS $_$
/*
  Создание дефолтных партиций для всех таблиц с партициями
*/
DECLARE
  table_name TEXT;
  table_new TEXT;
BEGIN
  FOR table_name IN SELECT
    c.relname
    from pg_catalog.pg_class c
    join pg_catalog.pg_namespace n on c.relnamespace = n.oid
    join pg_partitioned_table p on p.partrelid = c.oid
    order by n.nspname, c.relname
  LOOP
    table_new := format('%s_default', table_name);
    RAISE NOTICE '%: DEFAULT', table_new;
    if to_regclass(table_new) is null then
      -- создаем, если такого имени нет
      execute format('create table %I partition of %I default', table_new, table_name);
    else
      raise notice '  already exists';
    end if;
  END LOOP;
END
$_$;

CREATE OR REPLACE PROCEDURE enable_parts(
  table_name    TEXT
, table_column  TEXT
)
LANGUAGE plpgsql AS $_$
/*
  Конвертация таблицы table_name в партиционированную по полю table_column.
  ВНИМАНИЕ! Таблица будет переименована (добавится суффикс _pre) и в эту копию
  попадут все изменения, сделанные за время работы процедуры.
  После конвертации необходимо отдельно выполнить запрос
  INSERT INTO table SELECT * FROM table_pre EXCEPT SELECT * FROM table;
*/
DECLARE
  temp_table TEXT := table_name || '__temp';
BEGIN
  RAISE NOTICE '%: Enable partitions for %', table_name, table_column;
  execute format('create table %I (like %I including defaults including indexes) PARTITION BY RANGE (%I)'
    , temp_table, table_name, table_column);
  call create_parts(temp_table, child_prefix := table_name);
  RAISE NOTICE 'insert data..';
  execute format('insert into %I select * from %I', temp_table, table_name);

  RAISE NOTICE 'switch tables..';
  execute format('alter table %I rename to %I', table_name, table_name || '_pre');
  execute format('alter table %I rename to %I', temp_table, table_name);

  RAISE NOTICE '% is ready.', table_name;
END;
$_$;
