# CC=g++
# CFLAGS=-Wall -g -std=c++0x

# .SUFFIXES: .o .cpp .h

# SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/
# INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system

# CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -O0
# LDFLAGS = -Wall -L. -L./libs -pthread -g -lrt -std=c++0x -O0 -ljemalloc -no-pie
# LDFLAGS += $(CFLAGS)

# CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
# OBJS = $(CPPS:.cpp=.o)
# DEPS = $(CPPS:.cpp=.d)

# all:rundb

# rundb : $(OBJS)
# 	$(CC) -o $@ $^ $(LDFLAGS)

# -include $(OBJS:%.o=%.d)

# %.d: %.cpp
# 	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

# %.o: %.cpp
# 	$(CC) -c $(CFLAGS) -o $@ $<

# .PHONY: clean
# clean:
# 	rm -f rundb $(OBJS) $(DEPS)

CC = g++

CFLAGS = -Wall -g -std=c++11
LDFLAGS = -Wall -L. -L./libs -pthread -g -lrt -std=c++11 -O0 -ljemalloc -no-pie

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system

CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -O0
LDFLAGS += $(CFLAGS)

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))

OBJDIR = ./bulid/obj
DEPDIR = ./bulid/dep

# Makefile当中的函数形式是	$(function_name arg1, arg2, ...)
# 函数 $(patsubst args)是将模式文本替换
OBJS = $(patsubst ./%.cpp, $(OBJDIR)/%.o, $(CPPS))
DEPS = $(patsubst ./%.cpp, $(DEPDIR)/%.d, $(CPPS))

# 这里是用于指定的默认目标
all : rundb

# 目标通常是你想要构建的文件，比如一个可执行文件或者一个对象文件。
# 在某些情况下，目标也可以是一个标签，如clean或all，用于执行一组特定的命令而不是生成一个文件。
# 语法规则：
# <Target> : <Dependencies>
#		<others>是当依赖项更新或目标不存在时执行的命令。
# 并且这里的“$@”和“$^”分别表示了这里的目标名称以及所有依赖项
rundb : $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

-include $(DEPS)

$(DEPDIR)/%.d : %.cpp
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -MM $< -MT $(@:.d=.o) -MF $@

$(OBJDIR)/%.o : %.cpp
	mkdir -p $(dir $@)
	$(CC) $(CFLAGS) -c $< -o $@

# 第一行这个代码是将clean声明一个伪目标
.PHONY:clean
clean:
	rm -rf ./bulid
	rm -f rundb
