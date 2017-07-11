REBAR=rebar

get_deps:
	@./build_deps.sh

compile_nif: get_deps
	@make V=0 -C c_src -j 8

clean_nif:
	@make -C c_src clean

compile:
	${REBAR} compile

clean:
	${REBAR} clean


