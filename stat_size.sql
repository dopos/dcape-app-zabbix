
\qecho ** Список таблиц, у которых есть партиции
select
  n.nspname as schema
, c.relname as partition_name
              , pg_total_relation_size(c.oid) AS total_bytes
              , pg_indexes_size(c.oid) AS index_bytes
--              , pg_total_relation_size(reltoastrelid) AS toast_bytes

  from pg_catalog.pg_class c
  join pg_catalog.pg_namespace n on c.relnamespace = n.oid
  join pg_partitioned_table p on p.partrelid = c.oid
  order by n.nspname, c.relname
;

-- https://stackoverflow.com/a/52809725

\qecho ** Список партиций
with recursive inh as (
   select i.inhrelid, null::text as parent
   from pg_catalog.pg_inherits i
     join pg_catalog.pg_class cl on i.inhparent = cl.oid
     join pg_catalog.pg_namespace nsp on cl.relnamespace = nsp.oid
   where 
-- nsp.nspname = 'public'
--     and cl.relname like 'history%'
-- and 
cl.relkind <> 'I' -- not index
   union all
   select i.inhrelid, (i.inhparent::regclass)::text
   from inh
   join pg_catalog.pg_inherits i on (inh.inhrelid = i.inhparent)
)
select n.nspname as schema,
        c.relname as partition_name,
        pg_get_expr(c.relpartbound, c.oid, true) as partition_expression,
        pg_get_expr(p.partexprs, c.oid, true) as sub_partition,
        parent,
        case p.partstrat
          when 'l' then 'LIST'
          when 'r' then 'RANGE'
        end as sub_part_strat
              , pg_size_pretty(pg_total_relation_size(c.oid)) AS total_bytes
              , pg_size_pretty(pg_indexes_size(c.oid)) AS index_bytes
from inh
   join pg_catalog.pg_class c on inh.inhrelid = c.oid
   join pg_catalog.pg_namespace n on c.relnamespace = n.oid
   left join pg_partitioned_table p on p.partrelid = c.oid
order by n.nspname, c.relname
;
