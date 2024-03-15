from pyfunk.pyfunk import now, join, camel_to_snake
import logging

logger = logging.getLogger(__name__)


def instance_name(service):
    return camel_to_snake(type(service).__name__)


def generate_function_log(name, *args, **kwargs):
    from pyes.crud import ESCrudService
    all_args = []
    for arg in args:
        # This is a bit gross, being highly specific, but its useful for
        # a number of our timed queries
        if isinstance(arg, ESCrudService):
            all_args.append(instance_name(arg))
        else:
            all_args.append(arg)
    for k, v in kwargs.items():
        all_args.append("{k}={v}".format(k=k, v=v))

    prepared_args = join(all_args, ", ")

    return "{name}({args})".format(name=name, args=prepared_args)


def log_time(threshold=60000, console=False):
    def wrapper(f):
        function_name = "{0}.{1}".format(f.__module__, f.__name__)

        def wrapped(*args, **kwargs):
            start = now()
            result = f(*args, **kwargs)
            taken = now() - start
            if taken > threshold:
                first_line = "Function '{0}' took too long. Took: {1}, threshold: {2}".format(
                    function_name, taken, threshold
                )
                second_line = generate_function_log(function_name, *args, **kwargs)
                log = "[PROFILED] {0}\n{1}".format(first_line, second_line)
                logger.info(log)
                if console:
                    print(log)
            return result
        return wrapped
    return wrapper
