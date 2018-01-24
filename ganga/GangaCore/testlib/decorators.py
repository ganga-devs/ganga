import inspect


def add_config(config_values):
    # type: (Sequence[Tuple[str, str, Any]])
    """
    A decorator to label functions with config arguments for
    consumption by the ``ganga`` fixture.

    Sets the ``_config_values`` attribute on the function.

    If passed a class it will label all the functions within. Config
    entries labelled on a function in a class will take precedence
    over those labelled on the class itself.

    Args:
        config_values: a list of 3-tuples (config section, entry name, entry value)

    Example:
        .. code-block:: python

            @add_config([('Queues', 'NumWorkerThreads', 7)])
            def test_add_config(ganga):
                assert ganga.config['Queues'].NumWorkerThreads == 7
    """
    # Convert the list of tuples to a dict for easy updating and comparison
    config_map = dict(((section, name), value) for section, name, value in config_values)

    def add_config_wrapper(obj):
        if inspect.isfunction(obj):
            obj._config_values = getattr(obj, '_config_values', {})  # Set the value to a dict if it doesn't exist

            # Since function decorators are called before class
            # decorators, existing config items take precedence
            for new_item, new_value in config_map.items():
                if new_item not in obj._config_values:
                    obj._config_values[new_item] = new_value

        else:  # We're on a class object
            for item in obj.__dict__.values():
                if inspect.isfunction(item):  # Only add config to functions
                    add_config_wrapper(item)
        return obj

    return add_config_wrapper


@add_config([('Configuration', 'user', 'test')])
def test_add_config(gpi):
    assert gpi.config['Configuration'].user == 'test'


@add_config([('Configuration', 'user', 'test'), ('MSGMS', 'port', 43)])
class TestLayeredConfig(object):

    def test_class_config(self, gpi):
        assert gpi.config['Configuration'].user == 'test'
        assert gpi.config['MSGMS'].port == 43

    @add_config([('MSGMS', 'port', 42)])
    def test_config_override(self, gpi):
        assert gpi.config['Configuration'].user == 'test', 'Function-decorated configs should not remove all class ones'
        assert gpi.config['MSGMS'].port == 42, 'Function-decorated configs should take precedence over class ones'
