# Linux and similar...
CPUS=`getconf _NPROCESSORS_ONLN 2>/dev/null`
# FreeBSD and similar...
[ -z "$CPUS" ] && CPUS=`getconf NPROCESSORS_ONLN`
# Solaris and similar...
[ -z "$CPUS" ] && CPUS=`ksh93 -c 'getconf NPROCESSORS_ONLN'`
# Give up...
[ -z "$CPUS" ] && CPUS=1

get_deps:
	@./build_deps.sh

compile_nif: get_deps
	@make V=0 -C c_src -j $(CPUS)

clean_nif:
	@make -C c_src clean

C_SRC_DIR = $(shell pwd)/c_src
C_SRC_ENV ?= $(C_SRC_DIR)/env.mk

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

cpplint:
	cpplint --counting=detailed \
	        --filter=-legal/copyright,-build/include_subdir,-build/include_order,-whitespace/blank_line,-whitespace/braces,-whitespace/indent,-whitespace/parens,-whitespace/newline \
            --linelength=300 \
			--exclude=c_src/*.o --exclude=c_src/*.mk  \
			c_src/*.*

cppcheck:
	cppcheck -j $(CPUS) \
             -I /usr/local/opt/openssl/include \
             -I deps/librdkafka/src \
             -I $(ERTS_INCLUDE_DIR) \
             -I $(ERL_INTERFACE_INCLUDE_DIR) \
             --force \
             --enable=all \
	 		 --xml-version=2 \
	 		 --output-file=cppcheck_results.xml \
	 		 c_src/
