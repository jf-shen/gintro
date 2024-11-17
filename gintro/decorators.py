import time


def smart_time(sec):
    if sec < 120:
        return '%.2f sec' % sec
    min = sec / 60
    if min < 120:
        return '%.2f min' % min
    hour = min / 60
    return '%.2f h' % hour


def timeit(fn):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        print("[timeit] start running function [%s]" % fn.__name__)
        output = fn(*args, **kwargs)
        print("[timeit] finish running function [%s], time elapsed = %s" %
              (fn.__name__, smart_time(time.time() - start_time)))
        return output
    return wrapper


def time_it(name='name'):
    def inner(fn):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            output = fn(*args, **kwargs)
            print("[%s] %.3f" % (name, time.time() - start_time))
            return output

        return wrapper

    return inner

