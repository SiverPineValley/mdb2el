LIB = -L "C:\Users\Park\Anaconda3\libs" -I "C:\Users\Park\Anaconda3\include" -lpython36

mdb2el : mdb2el.o ini.o
	gcc -lm -o $@ $< ini.o $(LIB)
.c.o:
	gcc $(LIB) -g -c $^
mdb2el.o : mdb2el.c ini.c ini.h
ini.o : ini.c ini.h