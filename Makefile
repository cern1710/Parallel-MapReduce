CC = gcc
CFLAGS = -Wall -g # -fsanitize=address

OBJS = hashmap kvp

all: mapreduce

obj:
	@mkdir -p $@

obj/%.o: src/%.c obj
	$(CC) $(CFLAGS) -c $< -o $@

mapreduce: src/mapreduce.c $(patsubst %,obj/%.o,$(OBJS))
	$(CC) $(CFLAGS) $^ -o $@

clean:
	$(RM) mapreduce $(OBJS)

.PHONY: all clean