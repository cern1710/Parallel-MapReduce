CC = gcc
CFLAGS = -Wall -g # -fsanitize=address

OBJS = mapreduce kvlist

all: mapreduce

obj:
	@mkdir -p $@

obj/%.o: src/%.c obj
	$(CC) $(CFLAGS) -c $< -o $@

mapreduce: src/main.c $(patsubst %,obj/%.o,$(OBJS))
	$(CC) $(CFLAGS) $^ -o $@

clean:
	$(RM) mapreduce $(OBJS)

.PHONY: all clean