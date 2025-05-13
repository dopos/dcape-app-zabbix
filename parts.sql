/*

  Код для работы с партициями postgresql

  В имя партиции зашивается Unix timestamp. Так было в варианте для timescaledb, оставлено для совместимости

  Аналог из citus:
  SELECT create_time_partitions(table_name:= 'history',
    partition_interval:= '1 week',
    end_at:= '2023-11-16',
    start_from:= '2023-11-02');

  Аналог из timescaledb:
  SELECT create_hypertable('history', 'clock', chunk_time_interval => 86400, migrate_data => true);

  Код ниже имеет перемешанный регистр, т.к. капс для служебных слов потенциально устарел и
  окончательное решение по этому вопросу еще не принято.

*/

DROP SCHEMA IF EXISTS parts CASCADE;
CREATE SCHEMA IF NOT EXISTS parts;

CREATE OR REPLACE FUNCTION parts.date2uts(dt TEXT DEFAULT CURRENT_DATE) RETURNS INTEGER IMMUTABLE LANGUAGE sql AS $_$
  --  Конвертация даты в unix timestamp. Дата по умолчанию - текущая
  SELECT EXTRACT(EPOCH FROM dt::TIMESTAMP)
$_$;

/*

WITH v AS (
  SELECT parts.stamp2uts('2025-05-15 02:59:00 MSK'::timestamptz) AS uts
)
SELECT uts, parts.uts2stamp(uts)
  FROM v
;
    uts     |       uts2stamp
------------+------------------------
 1747267140 | 2025-05-15 02:59:00+03

*/

CREATE OR REPLACE FUNCTION parts.stamp2uts(ts TIMESTAMPTZ DEFAULT NULL) RETURNS INTEGER IMMUTABLE LANGUAGE sql AS $_$
  --  Конвертация времени в unix timestamp. Дата по умолчанию - текущая
  SELECT EXTRACT(EPOCH FROM COALESCE (
        ts
      , now()::timestamptz
      ))::INT
$_$;

CREATE OR REPLACE FUNCTION parts.uts2stamp(uts INTEGER) RETURNS timestamptz(0) IMMUTABLE LANGUAGE sql AS $_$
  --  Конвертация unix timestamp в timestamp
  select to_timestamp(uts)
$_$;

CREATE OR REPLACE FUNCTION parts.chunk_from(
  time_interval INT  DEFAULT 604800     -- 7 days
, time_min      INT  DEFAULT NULL
) RETURNS INT IMMUTABLE LANGUAGE sql AS $_$
  --  Расчет начального времени чанка для заданного момента
  SELECT time_interval * (
    COALESCE (
        time_min
      , extract (epoch from now()::timestamptz)::INT -- время в текущем поясе переводим в UTC
    ) / time_interval
  )::INT
$_$;

/*

select
  parts.uts2stamp(parts.chunk_from(time_min := parts.stamp2uts('2025-05-15 02:59:00 MSK'::timestamptz))) as chunk_start
, parts.uts2stamp(parts.chunk_from(time_min := parts.stamp2uts('2025-05-15 03:00:00 MSK'::timestamptz))) as next_chunk_start
;
      chunk_start       |    next_chunk_start
------------------------+------------------------
 2025-05-08 03:00:00+03 | 2025-05-15 03:00:00+03
(1 row)

select parts.uts2stamp(parts.stamp2uts ()) = now()::timestamptz(0) as eq;
 eq
----
 t
(1 row)

*/

CREATE OR REPLACE FUNCTION parts.chunk_from_mon(
  time_interval INT  DEFAULT 604800     -- 7 days
, time_min      INT  DEFAULT NULL
) RETURNS INT IMMUTABLE LANGUAGE sql AS $_$
  --  Расчет начального времени чанка (полночь понедельника по часовому поясу) для заданного момента
  SELECT time_interval * (
    ( COALESCE (
        time_min
      , extract (epoch from now()::timestamptz)::INT -- время в текущем поясе переводим в UTC
      )
    - extract (epoch from '1970-01-05'::timestamptz)::INT -- перед расчетом - убрать смещение на полночь понедельника по текущему часовому поясу
    ) / time_interval
  )::INT + extract (epoch from '1970-01-05'::timestamptz)::INT; -- вернуть смещение
$_$;

/*

select
  parts.uts2stamp(parts.chunk_from_mon(time_min := parts.stamp2uts('2025-05-11 23:59:00 MSK'::timestamptz))) as chunk_start
, parts.uts2stamp(parts.chunk_from_mon(time_min := parts.stamp2uts('2025-05-12 00:00:00 MSK'::timestamptz))) as next_chunk_start
;
      chunk_start       |    next_chunk_start
------------------------+------------------------
 2025-05-05 00:00:00+03 | 2025-05-12 00:00:00+03

*/

CREATE OR REPLACE VIEW parts.attached AS SELECT
  n.nspname
, c.relname
  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on c.relnamespace = n.oid
  join pg_partitioned_table p on p.partrelid = c.oid
  order by n.nspname, c.relname
;
COMMENT ON VIEW parts.attached IS 'Таблицы, у которых есть партиции';

CREATE OR REPLACE PROCEDURE parts.attach_table(
  table_name    TEXT
, schema_name   TEXT DEFAULT 'public'
, time_interval INT  DEFAULT 604800     -- 7 days
, chunk_count   INT  DEFAULT 2
, time_min      INT  DEFAULT NULL
, child_prefix  TEXT DEFAULT NULL
)
LANGUAGE plpgsql AS $_$
/*
  Создание партиций для таблицы schema_name.table_name
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
  -- округляем до заданного шага
  chunk_from := parts.chunk_from(time_interval, time_min);
  FOR i IN 1..chunk_count LOOP
    -- имя новой таблицы, префикс совпадает с текущей, если не задан явно
    table_new := format('%s_p%s', COALESCE(child_prefix, table_name), chunk_from);
    RAISE NOTICE '%.%: FROM % TO %', schema_name, table_new
            , parts.uts2stamp(chunk_from)
            , parts.uts2stamp(chunk_from + time_interval)
    ;
    if to_regclass(format('%I.%I', schema_name, table_new)) is null then
      -- создаем, если такого имени нет
      execute format('create table %I.%I partition of %I.%I for values from (%s) to (%s)'
              , schema_name, table_new, schema_name, table_name
              , chunk_from
              , chunk_from + time_interval
      );
    else
      raise notice '  already exists';
    end if;
    chunk_from := chunk_from + time_interval;
  END LOOP;
END
$_$;

CREATE OR REPLACE PROCEDURE parts.attach_default_table(
  table_name    TEXT
, schema_name   TEXT DEFAULT 'public'
, child_prefix  TEXT DEFAULT NULL
)
LANGUAGE plpgsql AS $_$
/*
  Создание дефолтной партиции для таблицы
*/
DECLARE
  table_new TEXT;
BEGIN
    table_new := format('%s_default', child_prefix);
    RAISE NOTICE '%.%: DEFAULT', schema_name, table_new;
    if to_regclass(format('%I.%I', schema_name, table_new)) is null then
      -- создаем, если такого имени нет
      execute format('create table %I.%I partition of %I.%I default', schema_name, table_new, schema_name, table_name);
    else
      raise notice '  already exists';
    end if;
END
$_$;

CREATE OR REPLACE PROCEDURE parts.attach_from_default(
  table_name    TEXT
, schema_name   TEXT DEFAULT 'public'
, time_interval INT  DEFAULT 604800     -- 7 days
, chunk_count   INT  DEFAULT 1
, time_min      INT  DEFAULT NULL
, child_prefix  TEXT DEFAULT NULL
)
LANGUAGE plpgsql AS $_$
/*
  Создание партиций для таблицы schema_name.table_name
  для случая, когда данные этой таблицы уже попали в DEFAULT партицию
  * заданный момент времени time_min [now()] округляется до time_interval
  * результат - это суффикс имени партиции и ее начальный интервал
  * конечный интервал определяется добавлением time_interval
  * если партиции с таким именем нет, она создается
  * начальный интервал увеличивается на time_interval
  * повторить chunk_count раз
*/
DECLARE
  chunk_from INT;
  chunk_max INT;
  i INT;
  table_new TEXT;
BEGIN
  -- округляем до заданного шага
  chunk_from := parts.chunk_from(time_interval, time_min);

  FOR i IN 1..chunk_count LOOP
    chunk_max := chunk_from + time_interval;

    IF parts.stamp2uts() BETWEEN chunk_from AND chunk_max THEN
      -- если текущая дата попадает в эту партицию
      RAISE NOTICE 'WARNING: если сейчас что-то пишет в дефолтную партицию, это может потеряться';
    END IF;
    -- имя новой таблицы, префикс совпадает с текущей, если не задан явно
    table_new := format('%s_p%s', COALESCE(child_prefix, table_name), chunk_from);
    RAISE NOTICE '%.%: FROM % TO %', schema_name, table_new
            , parts.uts2stamp(chunk_from)
            , parts.uts2stamp(chunk_max)
    ;
    IF to_regclass(format('%I.%I', schema_name, table_new)) IS NOT NULL THEN
      -- такое имя уже есть
      chunk_from := chunk_max;
      RAISE NOTICE 'exists';
      CONTINUE;
    END IF;

    -- создать отдельную таблицу
    RAISE NOTICE 'create';
    execute format('create table %I.%I (like %I.%I including defaults /*including indexes*/)'
            , schema_name, table_new, schema_name, table_name
    );
    -- добавить к ней чек
    RAISE NOTICE 'create check';
    execute format('alter table %I.%I add check (clock between %L AND %L)'
            , schema_name, table_new, chunk_from, chunk_max
    );
    -- перенести строки из дефолта
    RAISE NOTICE 'move data';
    execute format('WITH buffer AS (DELETE FROM %I.%I WHERE clock BETWEEN %L AND %L RETURNING *) INSERT INTO %I.%I SELECT * FROM buffer'
    , schema_name, table_name||'_default', chunk_from, chunk_max, schema_name, table_new
    );
    -- пристегнуть к родителю
    RAISE NOTICE 'attach';
    execute format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)'
    , schema_name, table_name, schema_name, table_new, chunk_from, chunk_max
    );
    chunk_from := chunk_max;
  END LOOP;
END
$_$;

CREATE OR REPLACE PROCEDURE parts.attach(
  a_schema_name TEXT DEFAULT NULL
, time_interval INT  DEFAULT 604800     -- 7 days
, chunk_count   INT  DEFAULT 2
, time_min      INT  DEFAULT NULL
, child_prefix  TEXT DEFAULT NULL
)
LANGUAGE plpgsql AS $_$
/*
  Создание партиций для всех таблиц с партициями или заданной схемы schema_name (если она NOT NULL)
*/
DECLARE
  schema_name TEXT;
  table_name TEXT;
BEGIN
  IF a_schema_name = '' THEN a_schema_name := NULL; END IF; -- из make удобнее передавать пустую строку
  FOR table_name, schema_name IN SELECT
    relname, nspname
    FROM parts.attached
    WHERE nspname = COALESCE(a_schema_name, nspname)
  LOOP
    call parts.attach_table(table_name, schema_name, time_interval, chunk_count, time_min);
  END LOOP;
END
$_$;

CREATE OR REPLACE PROCEDURE parts.defaults(
  a_schema_name   TEXT DEFAULT NULL
)
LANGUAGE plpgsql AS $_$
/*
  Создание дефолтных партиций для всех таблиц с партициями (если схема не задана) или для таблиц заданной схемы
*/
DECLARE
  schema_name TEXT;
  table_name TEXT;
  table_new TEXT;
BEGIN
  IF a_schema_name = '' THEN a_schema_name := NULL; END IF; -- из make удобнее передавать пустую строку
  FOR table_name, schema_name IN SELECT
    relname, nspname
    FROM parts.attached
    WHERE nspname = COALESCE(a_schema_name, nspname)
  LOOP
    table_new := format('%s_default', table_name);
    RAISE NOTICE '%.%: DEFAULT', schema_name, table_new;
    if to_regclass(format('%I.%I', schema_name, table_new)) is null then
      -- создаем, если такого имени нет
      execute format('create table %I.%I partition of %I.%I default', schema_name, table_new, schema_name, table_name);
    else
      raise notice '  already exists';
    end if;
  END LOOP;
END
$_$;

CREATE OR REPLACE PROCEDURE parts.defaults_size(
  a_schema_name   TEXT DEFAULT NULL
)
LANGUAGE plpgsql AS $_$
/*
  Проверка дефолтных партиций для всех таблиц с партициями (если схема не задана) или для таблиц заданной схемы
*/
DECLARE
  schema_name TEXT;
  table_name TEXT;
  table_new TEXT;
  clock_min INT;
  clock_max INT;
BEGIN
  IF a_schema_name = '' THEN a_schema_name := NULL; END IF; -- из make удобнее передавать пустую строку
  FOR table_name, schema_name IN SELECT
    relname, nspname
    FROM parts.attached
    WHERE nspname = COALESCE(a_schema_name, nspname)
  LOOP
    table_new := format('%s_default', table_name);
    execute format('select min(clock), max(clock) from %I.%I', schema_name, table_new) into clock_min,clock_max;
    IF COALESCE(clock_min,clock_max,-1) >0 THEN
      RAISE NOTICE 'WARNING: table %.% has not empty default partition (% - %)'
      , schema_name, table_new
      , parts.uts2stamp(clock_min)
      , parts.uts2stamp(clock_max)
      ;
    END IF;
  END LOOP;
END
$_$;

CREATE OR REPLACE PROCEDURE parts.enable(
  table_name    TEXT
, table_column  TEXT
, schema_name   TEXT DEFAULT 'public'
)
LANGUAGE plpgsql AS $_$
/*
  Конвертация таблицы schema_name.table_name в партиционированную по полю table_column.
*/
DECLARE
  temp_table TEXT := table_name || '__temp';
BEGIN
  RAISE NOTICE '%.%: Enable partitions for %', schema_name, table_name, table_column;
  execute format('create table %I.%I (like %I.%I including defaults including indexes) PARTITION BY RANGE (%I)'
    , schema_name, temp_table, schema_name, table_name, table_column);
  call parts.attach_default_table(temp_table, schema_name, child_prefix := table_name);
  call parts.attach_table(temp_table, schema_name, child_prefix := table_name);
  -- TODO: добавить партиции для всех данных исходной таблицы
  RAISE NOTICE 'insert data..';
  execute format('WITH buffer AS (DELETE FROM %I.%I RETURNING *) INSERT INTO %I.%I SELECT * FROM buffer'
    , schema_name, table_name, schema_name, temp_table);

  RAISE NOTICE 'switch tables..';
  execute format('alter table %I.%I rename to %I', schema_name, table_name, table_name || '_pre');
  execute format('alter table %I.%I rename to %I', schema_name, temp_table, table_name);

  -- С момента первого INSERT в эту таблицу могли что-то писать, поэтому, переименовав, повторим
  RAISE NOTICE 'insert new data..';
  -- TODO: собрать кейс, где эта выборка будет не пустой
  execute format('WITH buffer AS (DELETE FROM %I.%I RETURNING *) INSERT INTO %I.%I SELECT * FROM buffer'
    , schema_name, table_name || '_pre', schema_name, table_name);
  execute format('DROP TABLE %I.%I', schema_name, table_name || '_pre'); -- пустая таблица, TODO: проверить перед удалением
  RAISE NOTICE '%.% is ready.', schema_name, table_name;
END;
$_$;

CREATE OR REPLACE PROCEDURE parts.move(
  schema_old   TEXT
, schema_new   TEXT
, time_min     INT
, time_interval INT  DEFAULT 604800     -- 7 days
, index_suffix TEXT DEFAULT '_itemid_clock_idx'
)
LANGUAGE plpgsql AS $_$
/*
  Перенос партиции времени time_min из схемы a_schema_old в a_schema_new
  Пример вызова:
    call parts.move('s01', 'public', 1701907200);
*/
DECLARE
  chunk_from INT;
  chunk_max INT;
  table_new TEXT;
  table_name TEXT;
BEGIN
  -- округляем до заданного шага
  chunk_from := parts.chunk_from(time_interval, time_min);
  chunk_max := chunk_from + time_interval;
  FOR table_name IN SELECT
    relname
    FROM parts.attached
    WHERE nspname = schema_old
  LOOP
    RAISE NOTICE '%_p%: % -> %', table_name, chunk_from, schema_old, schema_new;
    table_new := format('%s_p%s', table_name, chunk_from);
    if to_regclass(format('%I.%I', schema_old, table_new)) is null then
      raise notice '  not found';
    else
      if to_regclass(format('%I.%I', schema_new, table_new)) is not null then
        -- detach partition in new
        execute format('alter table %I.%I detach partition %I.%I', schema_new, table_name, schema_new, table_new);
        execute format('alter table %I.%I rename to %I', schema_new, table_new, table_new||'_back');
        execute format('alter index if exists %I.%I rename to %I', schema_new, table_new || '_pkey'
        , table_new || '_back_pkey');
        execute format('drop index if exists %I.%I', schema_new, table_new || index_suffix);
      end if;
      execute format('alter table %I.%I detach partition %I.%I', schema_old, table_name, schema_old, table_new);
      execute format('alter table %I.%I set schema %I', schema_old, table_new, schema_new);
      execute format('ALTER TABLE %I.%I ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)', schema_new, table_name, schema_new, table_new, chunk_from, chunk_max);
    end if;
  END LOOP;
END
$_$;

CREATE OR REPLACE PROCEDURE parts.set_table_columnar(
  a_schema_name   TEXT DEFAULT NULL
, time_interval INT  DEFAULT 604800     -- 7 days
, chunk_count   INT  DEFAULT 1
, time_min      INT  DEFAULT NULL
, is_off        BOOL DEFAULT FALSE      -- отменить columnar
)
LANGUAGE plpgsql AS $_$
/*
  Изменение метода хранения columnar/heap для всех партиций заданного чанка
*/
DECLARE
  mode        TEXT := 'columnar';
  schema_name TEXT;
  table_name  TEXT;
  table_new   TEXT;
  chunk_from  INT;
  chunk       INT;
  i           INT;
BEGIN
  IF a_schema_name = '' THEN a_schema_name := NULL; END IF; -- из make удобнее передавать пустую строку
  IF is_off THEN mode := 'heap'; END IF;
  -- округляем до заданного шага
  chunk_from := parts.chunk_from(time_interval, time_min);
  FOR table_name, schema_name IN SELECT
    relname, nspname
    FROM parts.attached
    WHERE nspname = COALESCE(a_schema_name, nspname)
  LOOP
    FOR i IN 1..chunk_count LOOP
      chunk := chunk_from + time_interval * (i-1);
      table_new := format('%s_p%s', table_name, chunk);
      IF to_regclass(format('%I.%I', schema_name, table_new)) IS NOT NULL THEN
        RAISE NOTICE '%.%: SET %', schema_name, table_new, mode;
        PERFORM alter_table_set_access_method(format('%I.%I', schema_name, table_new), mode);
      ELSE
        RAISE NOTICE '%.%: not found', schema_name, table_new;
      END IF;
    END LOOP;
  END LOOP;
END;
$_$;

CREATE TYPE parts.utsinfo AS (
  uts INT
, stamp TIMESTAMPTZ(0)
);

CREATE OR REPLACE FUNCTION parts.uts_info(
  time_min      INT  DEFAULT NULL
, chunk_count   INT  DEFAULT 1
, time_interval INT  DEFAULT 604800     -- 7 days
) RETURNS SETOF parts.utsinfo
LANGUAGE sql AS $_$
-- Посчитать uts для чанков
WITH v AS (
  SELECT  num, parts.chunk_from(time_interval, COALESCE (
        time_min
      , extract (epoch from now()::timestamptz)::INT -- время в текущем поясе переводим в UTC
    ) + time_interval * (num - 1)) as uts
  FROM    generate_series(1, chunk_count) num
)
SELECT uts, parts.uts2stamp(uts)
  FROM v
;
$_$;

/*
select * from parts.uts_info();
    uts     |         stamp
------------+------------------------
 1746662400 | 2025-05-08 03:00:00+03
(1 row)


select * from parts.uts_info(chunk_count:=3);
    uts     |         stamp
------------+------------------------
 1746662400 | 2025-05-08 03:00:00+03
 1747267200 | 2025-05-15 03:00:00+03
 1747872000 | 2025-05-22 03:00:00+03
(3 rows)

select * from parts.uts_info(time_min:=parts.date2uts('2025-04-01'), chunk_count:=6);
    uts     |         stamp
------------+------------------------
 1743033600 | 2025-03-27 03:00:00+03
 1743638400 | 2025-04-03 03:00:00+03
 1744243200 | 2025-04-10 03:00:00+03
 1744848000 | 2025-04-17 03:00:00+03
 1745452800 | 2025-04-24 03:00:00+03
 1746057600 | 2025-05-01 03:00:00+03

*/
