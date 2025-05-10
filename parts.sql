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

*/

DROP SCHEMA IF EXISTS parts CASCADE;
CREATE SCHEMA IF NOT EXISTS parts;

CREATE OR REPLACE FUNCTION parts.date2uts(dt TEXT DEFAULT CURRENT_DATE) RETURNS INTEGER IMMUTABLE LANGUAGE sql AS $_$
  --  Конвертация даты в unix timestamp. Дата по умолчанию - текущая
  SELECT EXTRACT(EPOCH FROM dt::TIMESTAMP)
$_$;

CREATE OR REPLACE FUNCTION parts.uts2date(uts INTEGER) RETURNS timestamp(0) IMMUTABLE LANGUAGE sql AS $_$
  --  Конвертация даты в unix timestamp
  SELECT to_timestamp(uts)
$_$;

CREATE OR REPLACE FUNCTION parts.chunk_from(
  time_interval INT  DEFAULT 604800     -- 7 days
, time_min      INT  DEFAULT NULL
) RETURNS INT IMMUTABLE LANGUAGE sql AS $_$
  --  Расчет начального времени чанка для заданного момента
  SELECT time_interval * (time_min / time_interval)::INT - 4 * 3600; -- начало чанка - предыдущий понедельник
$_$;

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
  IF time_min IS NULL THEN
    -- если начальное время не задано, берем текущее
    time_min := extract(epoch from now());
  END IF;
  -- округляем до заданного шага
  chunk_from := parts.chunk_from(time_interval, time_min);
  FOR i IN 1..chunk_count LOOP
    -- имя новой таблицы, префикс совпадает с текущей, если не задан явно
    table_new := format('%s_p%s', COALESCE(child_prefix, table_name), chunk_from);
    RAISE NOTICE '%.%: FROM % TO %', schema_name, table_new
            , parts.uts2date(chunk_from)
            , parts.uts2date(chunk_from + time_interval)
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
  schema_name TEXT;
  table_name TEXT;
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
  IF time_min IS NULL THEN
    -- если начальное время не задано, берем текущее
    time_min := extract(epoch from now());
  END IF;
  -- округляем до заданного шага
  chunk_from := parts.chunk_from(time_interval, time_min);

  FOR i IN 1..chunk_count LOOP
    chunk_max := chunk_from + time_interval;

    IF extract(epoch from now()) BETWEEN chunk_from AND chunk_max THEN
      -- если текущая дата попадает в эту партицию
      RAISE NOTICE 'WARNING: если сейчас что-то пишет в дефолтную партицию, это может потеряться';
    END IF;
    -- имя новой таблицы, префикс совпадает с текущей, если не задан явно
    table_new := format('%s_p%s', COALESCE(child_prefix, table_name), chunk_from);
    RAISE NOTICE '%.%: FROM % TO %', schema_name, table_new
            , parts.uts2date(chunk_from)
            , parts.uts2date(chunk_max)
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
      , parts.uts2date(clock_min)
      , parts.uts2date(clock_max)
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
  ВНИМАНИЕ! Таблица будет переименована (добавится суффикс _pre) и в эту копию
  попадут все изменения, сделанные за время работы процедуры.
  После конвертации необходимо отдельно выполнить запрос
  INSERT INTO table SELECT * FROM table_pre EXCEPT SELECT * FROM table;
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
  execute format('insert into %I.%I select * from %I.%I', schema_name, temp_table, schema_name, table_name);

  RAISE NOTICE 'switch tables..';
  execute format('alter table %I.%I rename to %I.%I', schema_name, table_name, schema_name, table_name || '_pre');
  execute format('alter table %I.%I rename to %I.%I', schema_name, temp_table, schema_name, table_name);

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

-- Cleanup old
CREATE OR REPLACE PROCEDURE parts.drop_old_proc() LANGUAGE plpgsql AS $_$
BEGIN
  DROP PROCEDURE IF EXISTS create_parts(text,text,integer,integer,integer,text);
  DROP PROCEDURE IF EXISTS create_default_parts_for_all();
  DROP PROCEDURE IF EXISTS create_default_parts_for_all(text);
  DROP PROCEDURE IF EXISTS create_parts(text,text,int,int,int,text);
  DROP PROCEDURE IF EXISTS create_parts(text,int,int,int,text);
  DROP PROCEDURE IF EXISTS create_parts_for_all(int,int,int,text);
  DROP PROCEDURE IF EXISTS create_parts_for_schema(text,int,int,int,text);
  DROP PROCEDURE IF EXISTS enable_parts(text,text);
END
$_$;

