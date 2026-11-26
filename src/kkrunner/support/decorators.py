def portable(function):
    """Mark a function whose source may be sent to a remote interpreter.

    This declaration does not validate the function. The direct-backend tests
    verify portability without adding import-time or transport overhead.
    """
    function.__kkrunner_portable__ = True
    # @depends_on may be applied before or after @portable.
    function.__dict__.setdefault("__source_dependencies__", ())
    return function


def depends_on(*functions):
    """Declare function sources needed when this function is delivered remotely.

    Dependencies must be top-level functions, referenced by their own names.
    Local execution is unchanged. Source delivery validates the complete graph
    and rejects undeclared globals rather than copying a module namespace.
    """
    def decorate(function):
        function.__source_dependencies__ = tuple(functions)
        return function
    return decorate


class class_property:
    """
    Decorator that converts a method with a single cls argument into a property
    that can be accessed directly from the class. The value is not cached, see
    :func:`cached_class_property`, if caching is desirable.

    Example
    -------

    .. code-block::

       class Cls:
          @class_property
          def cls_property():
              return some_value

          x = Cls.cls_property
    """

    def __init__(self, method=None):
        self.fget = method

    def __get__(self, instance, cls=None):
        return self.fget(cls)
