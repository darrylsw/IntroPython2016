def function_builder(n):
    return [lambda x, y=i: x + y for i in range(n)]
