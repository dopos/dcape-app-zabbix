## dcape-app-template Makefile
## This file extends Makefile.app from dcape
#:

SHELL               = /bin/bash
CFG                ?= .env
CFG_BAK            ?= $(CFG).bak

#- App name
APP_NAME           ?= service-template

#- Docker image name
IMAGE              ?= ghcr.io/lekovr/service-template

#- Docker image tag
IMAGE_VER          ?= 0.1.0

# If you need database, uncomment this var
#USE_DB              = yes

# If you need user name and password, uncomment this var
#ADD_USER            = yes

# ------------------------------------------------------------------------------

# if exists - load old values
-include $(CFG_BAK)
export

-include $(CFG)
export

# This content will be added to .env
# define CONFIG_CUSTOM
# # ------------------------------------------------------------------------------
# # Sample config for .env
# #SOME_VAR=value
#
# endef

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

## Template support code, used once
use-template:

