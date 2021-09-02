all: main

CXX := g++
LD := ${CXX}

# Flags to enable link-time optimization and GDB
LTO := -flto
ENABLE_DGB := 

HOTS_HOME := ../..
MICA_HOME := ../../mica2/src

# Boost specific.
BOOST_INC := /home/jacob/.conan/data/boost/1.69.0/_/_/package/cfef62e446e1bb07894de4523f2649afdfa94524/include
BOOST_LD := /home/jacob/.conan/data/boost/1.69.0/_/_/package/cfef62e446e1bb07894de4523f2649afdfa94524/lib
BOOST_WARN := -DBOOST_COROUTINES_NO_DEPRECATION_WARNING

INC	:= -I ${HOTS_HOME} -I ${MICA_HOME} -I ${BOOST_INC} ${BOOST_WARN}
LDPATH := -L${BOOST_LD}

# DEBUG := -DNDEBUG
CPPFLAGS := ${ENABLE_DGB} ${LTO} -g -O0 ${DEBUG} -std=c++11 ${INC} -Wall -Werror \
	-Wno-unused-result -Wno-unused-value -Wno-unused-function \
	-Wno-class-memaccess -Wno-deprecated-declarations -Winline -faligned-new

LDFLAGS := ${ENABLE_DGB} ${LTO} -Og ${LDPATH} -libverbs -lrt -pthread -lmemcached -lnuma \
	-lpapi -lboost_system -lboost_coroutine -lboost_thread -lboost_context

src := ${HOTS_HOME}/libhrd/hrd_conn.o ${HOTS_HOME}/libhrd/hrd_util.o \
	${HOTS_HOME}/rpc/rpc.o ${HOTS_HOME}/rpc/rpc_datapath.o \
	${HOTS_HOME}/rpc/rpc_lockserver.o \
	main.o worker.o

# Handle MICA differently. MICA relies on DNDEBUG to disable datapath asserts.
MICA_DEBUG := -DNDEBUG
mica_src := ${MICA_HOME}/mica/util/config.o \
	${MICA_HOME}/mica/util/cityhash/city_mod.o \
	${MICA_HOME}/mica/util/zipf.o \
	${MICA_HOME}/mica/util/lcore.o \
	${MICA_HOME}/mica/alloc/hugetlbfs_shm.o

${mica_src} : CPPFLAGS += ${MICA_DEBUG}

main: ${src} ${mica_src}
	${LD} -o $@ $^ ${LDFLAGS}

PHONY: clean
clean:
	rm -f *.o main ${src} ${mica_src}
