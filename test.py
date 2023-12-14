try:
    1 / 0
except Exception as e:
    print(f'{e.__class__.__name__}: {repr(e)}')

