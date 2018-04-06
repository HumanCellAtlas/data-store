import copy
import typing


class StateMachineAnnotation:
    pass


class ThreadPoolAnnotation(StateMachineAnnotation):
    """
    This annotation binds a state machine state to another state machine, which is replicated across `N` replicas, where
    N=`pool_size`.  The states in the nested state machine should have `XYZ` in their name, where
    XYZ=`template_string`.  These will be replaced with the branch number in the outputted state machine.
    """
    def __init__(self, state_machine: dict, pool_size: int, template_string: str) -> None:
        self.state_machine = state_machine
        self.pool_size = pool_size
        self.template_string = template_string

    def __deepcopy__(self, memodict) -> "ThreadPoolAnnotation":
        state_machine_copy = copy.deepcopy(self.state_machine, memodict)
        pool_size_copy = copy.deepcopy(self.pool_size, memodict)
        template_string_copy = copy.deepcopy(self.template_string, memodict)
        return self.__class__(state_machine_copy, pool_size_copy, template_string_copy)


class BranchIdTransformationAnnotation(StateMachineAnnotation):
    """
    This annotation references a template string that is replaced with the branch number.  Rather than having it
    transformed to a string with the branch number, it is transformed based on a callback and recorded in the final
    state machine definition.
    """
    def __init__(self, template_string: str, callable: typing.Callable[[int], typing.Any]) -> None:
        self.template_string = template_string
        self.callable = callable

    def __deepcopy__(self, memodict) -> "BranchIdTransformationAnnotation":
        return self.__class__(
            copy.deepcopy(self.template_string),
            copy.deepcopy(self.callable),
        )
