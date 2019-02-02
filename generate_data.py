import os

from random import randint

if __name__ == '__main__':
    DAY = 4
    for i in range(1, DAY):
        day_file = open("data/day={}".format(i), 'w', encoding='utf-8')
        for v in range(0, 100):
            value = randint(0, 1000)
            day_file.write("k{},{}\n".format(value, value))

        day_file.close()