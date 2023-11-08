## dcape-app-zabbix Makefile
## This file extends Makefile.app from dcape
#:

SHELL               = /bin/bash
CFG                ?= .env
CFG_BAK            ?= $(CFG).bak

#- App name
APP_NAME           ?= zabbix

#- Docker frontend image name
IMAGE              ?= zabbix/zabbix-web-nginx-pgsql

#- Docker image tag (all images)
IMAGE_VER          ?= alpine-6.2.7

# If you need database, uncomment this var
USE_DB              = yes

# If you need user name and password, uncomment this var
#ADD_USER            = yes

# Other docker images
SERVER_IMAGE       ?= zabbix/zabbix-server-pgsql
AGENT_IMAGE        ?= zabbix/zabbix-agent

# ------------------------------------------------------------------------------

# if exists - load old values
-include $(CFG_BAK)
export

-include $(CFG)
export

# This content will be added to .env
define CONFIG_CUSTOM
# ------------------------------------------------------------------------------
# Zabbix config addon

SERVER_IMAGE=$(SERVER_IMAGE)
AGENT_IMAGE=$(AGENT_IMAGE)

PHP_TZ=$(TZ)

DB_CONTAINER=$(DB_CONTAINER)

endef

# ------------------------------------------------------------------------------
# Find and include DCAPE_ROOT/Makefile
DCAPE_COMPOSE   ?= dcape-compose
DCAPE_ROOT      ?= $(shell docker inspect -f "{{.Config.Labels.dcape_root}}" $(DCAPE_COMPOSE))

ifeq ($(shell test -e $(DCAPE_ROOT)/Makefile.app && echo -n yes),yes)
  include $(DCAPE_ROOT)/Makefile.app
else
  include /opt/dcape/Makefile.app
endif

# ------------------------------------------------------------------------------
## App operations
#:

sql:
	@cat $${SQL:?Must be set} | docker exec -i $${DB_CONTAINER:?Must be set} psql -d $$PGDATABASE -U $$PGUSER

## Бэкап партиций с данными на вчера
dump-parts:
	@week=$$(expr $$(date --date=yesterday +%s) / 604800) ; from=$$(expr $$week \* 604800) ; echo "BackUp parts for $$from..." ; \
	docker exec -i $$DB_CONTAINER pg_dump -d $$PGDATABASE -U $$PGUSER -t '*_p'$${from} -Ft | gzip > backup_parts_$${from}.tgz

## Бэкап БД без партиций
dump-noparts:
	@week=$$(expr $$(date --date=yesterday +%s) / 604800) ; from=$$(expr $$week \* 604800) ; echo "BackUp data for $$from..." ; \
	docker exec -i $$DB_CONTAINER pg_dump -d $$PGDATABASE -U $$PGUSER -n public -T '*_p[0-9]+' -Ft | gzip > backup_noparts_$${from}.tgz

## Восстановление архива из параметра SRC
rest:
	zcat $${SRC:?Must be set} | docker exec -i $$DB_CONTAINER pg_restore -Ft -O -d $$PGDATABASE -U $$PGUSER

## Загрузить вспомогательный код
parts-install:
	@cat parts.sql | docker exec -i $${DB_CONTAINER:?Must be set} psql -d $$PGDATABASE -U $$PGUSER

## Создание новых партиций
parts-new:
	@docker exec -i $${DB_CONTAINER:?Must be set} psql -d $$PGDATABASE -U $$PGUSER -c 'call create_parts_for_all()'

