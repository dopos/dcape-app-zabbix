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
#IMAGE_VER          ?= alpine-6.4.8
IMAGE_VER          ?= alpine-6.4-latest

# If you need database, uncomment this var
USE_DB              = yes

# If you need user name and password, uncomment this var
#ADD_USER            = yes

# Other docker images
SERVER_IMAGE       ?= zabbix/zabbix-server-pgsql
AGENT_IMAGE        ?= zabbix/zabbix-agent2

DATESTAMP          ?= $(shell date +%F_%H%M)
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

include Makefile.parts

# Inclide local targets (if exists)
-include Makefile.local

# ------------------------------------------------------------------------------
## App support operations
#:

## top via pgcenter
top:
	@docker run -it --rm --network ${DCAPE_NET} -e PGPASSWORD=$$PGPASSWORD \
	lesovsky/pgcenter:latest pgcenter top -h db -U $$PGUSER -d $$PGDATABASE

## Отключить алертинг (при запуске копии на продовых данных)
alerts-off:
	@cat alerts-off.sql | docker exec -i $${DB_CONTAINER:?Must be set} psql -d $$PGDATABASE -U $$PGUSER -n $${PGSCHEMA}

