import copy
import typing

import domovoi

from . import types


class StateMachineAnnotationProcessor:
    """
    This class transforms a state machine definition, annotated with the annotations defined in
    `dss.stepfunctions.generator.types`, into a state machine definition supported by Domovoi.
    """

    def __init__(
            self,
            app: domovoi.Domovoi,
            template_map: typing.MutableSequence[typing.Tuple[str, int]]=None,
    ) -> None:
        self.app = app
        self.template_map = list() if template_map is None else template_map

    def process_annotations(self, annotated_state_machine: dict) -> dict:
        return self._process_single_field(annotated_state_machine)

    def _process_single_field(
            self,
            state_machine_element: typing.Any,
    ) -> typing.Any:
        if isinstance(state_machine_element, dict):
            return self._process_dict(state_machine_element)
        elif isinstance(state_machine_element, list):
            return self._process_array(state_machine_element)
        else:
            return self._process_singleton(state_machine_element)

    def _process_dict(
            self,
            state_machine_element: dict,
    ) -> dict:
        result = dict()
        for key, value in state_machine_element.items():
            transformed_key = self._process_str(key)
            result[transformed_key] = self._process_single_field(value)
        return result

    def _process_array(
            self,
            state_machine_element: typing.Sequence,
    ) -> list:
        result = list()
        for ix, value in enumerate(state_machine_element):
            result.append(self._process_single_field(value))
        return result

    def _process_singleton(
            self,
            state_machine_element: typing.Any,
    ) -> typing.Any:
        if isinstance(state_machine_element, types.ThreadPoolAnnotation):
            results = []
            for ix in range(state_machine_element.pool_size):
                template_map = copy.deepcopy(self.template_map)
                template_map.append((state_machine_element.template_string, ix))

                transformer = StateMachineAnnotationProcessor(self.app, template_map)
                results.append(transformer.process_annotations(state_machine_element.state_machine))

            return results
        elif isinstance(state_machine_element, types.StateMachineAnnotation):
            raise TypeError("Unhandled state machine annotation")
        elif isinstance(state_machine_element, str):
            return self._process_str(state_machine_element)
        elif callable(state_machine_element):
            branch_id = typing.cast(typing.Tuple[int], tuple([index for _, index in self.template_map]))

            # This is a workaround for python's static but lexical scope.  If we don't do this, we'll bind to the *last*
            # dynamic value of branch_id in this method.  That is, if _process_singleton is called multiple times, the
            # scoping rules will cause the lambda to pick up the value of the last call to _process_singleton, and not
            # the value at the time the lambda was created.
            if len(branch_id) > 0:
                return (lambda method, branch_id: lambda event, context, *args, **kwargs: method(
                    event, context, branch_id, *args, **kwargs))(state_machine_element, branch_id)
            else:
                return state_machine_element
        else:
            return state_machine_element

    def _process_str(self, string: str):
        for template_string, index in self.template_map:
            string = string.replace(template_string, str(index))

        return string
