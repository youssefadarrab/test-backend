from enum import StrEnum


class LowerStrEnum(StrEnum):
    """String enum whose value is the lowercased member name. Use with auto():

        class Color(LowerStrEnum):
            RED = auto()      # Color.RED == "red"
    """

    def _generate_next_value_(name, *_):
        return name.lower()
