# Based on c_src.mk from erlang.mk by Loic Hoguin <essen@ninenines.eu>
# https://github.com/ninenines/erlang.mk/blob/master/plugins/c_src.mk

CURDIR := $(shell pwd)
BASEDIR := $(abspath $(CURDIR)/..)

ifndef REBAR_BARE_COMPILER_OUTPUT_DIR
	PRIV_DIR ?= $(BASEDIR)/priv
else
	PRIV_DIR ?= $(REBAR_BARE_COMPILER_OUTPUT_DIR)/priv
endif

C_SRC_DIR = $(CURDIR)
C_SRC_ENV ?= $(C_SRC_DIR)/env.mk
C_SRC_OUTPUT ?= $(PRIV_DIR)/$(PROJECT_NIF_NAME).so

#regenerate all the time the env.mk
ifneq ($(wildcard $(C_SRC_DIR)),)
	GEN_ENV ?= $(shell erl -noshell -s init stop -eval "file:write_file(\"$(C_SRC_ENV)\", \
		io_lib:format( \
			\"ERTS_INCLUDE_DIR ?= ~s/erts-~s/include/~n\" \
			\"ERL_INTERFACE_INCLUDE_DIR ?= ~s~n\" \
			\"ERL_INTERFACE_LIB_DIR ?= ~s~n\", \
			[code:root_dir(), erlang:system_info(version), \
			code:lib_dir(erl_interface, include), \
			code:lib_dir(erl_interface, lib)])), \
		halt().")
    $(GEN_ENV)
endif

include $(C_SRC_ENV)

# System type and C compiler/flags.

UNAME_SYS_ORG := $(shell uname -s)
UNAME_SYS = $(shell echo $(UNAME_SYS_ORG) | tr A-Z a-z)

ifeq ($(UNAME_SYS), darwin)
    CC ?= cc
	CFLAGS ?= -O3 -std=c99 -finline-functions -Wall
	CXXFLAGS ?= -O3 -Wall
    LDFLAGS ?=
else ifeq ($(UNAME_SYS), freebsd)
	CC ?= cc
	CFLAGS ?= -O3 -std=c99 -finline-functions -Wall
	CXXFLAGS ?= -O3 -finline-functions -Wall
    LDFLAGS ?= -Wl,--exclude-libs=ALL
else ifeq ($(UNAME_SYS), linux)
	CC ?= gcc
	CFLAGS ?= -O3 -std=c99 -finline-functions -Wall
	CXXFLAGS ?= -O3 -finline-functions -Wall
    LDFLAGS ?= -Wl,--exclude-libs=ALL
endif

CFLAGS += -fPIC -I $(ERTS_INCLUDE_DIR) -I $(ERL_INTERFACE_INCLUDE_DIR)
CXXFLAGS += -fPIC -I $(ERTS_INCLUDE_DIR) -I $(ERL_INTERFACE_INCLUDE_DIR)
LDFLAGS += -L $(ERL_INTERFACE_LIB_DIR) -shared -lei

# Verbosity.

c_verbose_0 = @echo " C     " $(?F);
c_verbose = $(c_verbose_$(V))

cpp_verbose_0 = @echo " CPP   " $(?F);
cpp_verbose = $(cpp_verbose_$(V))

link_verbose_0 = @echo " LD    " $(@F);
link_verbose = $(link_verbose_$(V))

SOURCES := $(shell find $(C_SRC_DIR) -type f \( -name "*.c" -o -name "*.C" -o -name "*.cc" -o -name "*.cpp" \))
OBJECTS = $(addsuffix .o, $(basename $(SOURCES)))

COMPILE_C = $(c_verbose) $(CC) $(CFLAGS) $(CPPFLAGS) -c
COMPILE_CPP = $(cpp_verbose) $(CXX) $(CXXFLAGS) $(CPPFLAGS) -c

$(C_SRC_OUTPUT): $(OBJECTS)
	@mkdir -p $(PRIV_DIR)/
	$(link_verbose) $(CC) $(OBJECTS) $(LDFLAGS) -o $(C_SRC_OUTPUT)

%.o: %.c
	$(COMPILE_C) $(OUTPUT_OPTION) $<

%.o: %.cc
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

%.o: %.C
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

%.o: %.cpp
	$(COMPILE_CPP) $(OUTPUT_OPTION) $<

clean:
	@rm -f $(C_SRC_OUTPUT) $(OBJECTS); rm -f $(C_SRC_ENV)
