class intsum():
    def __init__(self):
        self.current = 0
        self.sum = 0

    def __iter__(self):
        return self

    def __next__(self):
        self.sum += self.current
        self.current += 1
        return self.sum

def intsum2():
    sum = 0
    current = 0
    while True:
        sum += current
        yield sum
        current += 1


def doubler():
    num = 1
    while True:
        yield num
        num *= 2


def fib():
    x, y = 0, 1
    while True:
        yield y
        x, y, = y, x + y


def prime():
    current = 2
    while True:
        is_prime = True
        for x in range(2, current):
            if current % x == 0:
                is_prime = False
                break
        if is_prime:
            yield current
        else:
            pass
        current += 1
