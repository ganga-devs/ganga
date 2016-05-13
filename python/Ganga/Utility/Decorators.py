def static_variable(name, default_value=None):
    """decorator to add a static variable to a function

    Example:

        @static_variable('myvar', 5)
        def myfunc():
            print myfunc.myvar  # = 5

    Args:
        name (str): Name of the static variable
        default_value (Optional): The default value of the static. Note that this is evaluated on *import* not on first
            call

    """

    def wrap(f):
        setattr(f, name, default_value)
        return f
    return wrap
