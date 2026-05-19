import pprint

DEBUGGING = False


def debug(*args):
    if DEBUGGING:
        print(*args)


def ppdegub(*args):
    if DEBUGGING:
        pprint.pprint(*args)