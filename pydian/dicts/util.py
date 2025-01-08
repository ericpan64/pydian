from pathlib import Path
from typing import Any, Iterable, Sequence, TypeAlias, Union

from parsimonious.grammar import Grammar
from parsimonious.nodes import Node, NodeVisitor

from ..lib.types import DROP, KEEP
from ..lib.util import flatten_sequence

GET_DSL_GRAMMAR = Grammar(Path(__file__).parent.joinpath("dsl/get.peg").read_text())

GetDslTreeResults: TypeAlias = Union[str, int, slice, list[str], tuple[str, ...]]


def get_keys_containing_class(source: dict[str, Any], cls: type, key_prefix: str = "") -> set[str]:
    """
    Recursively finds all keys where a DROP object is found.
    """
    res = set()
    for k, v in source.items():
        curr_key = f"{key_prefix}.{k}" if key_prefix != "" else k
        match v:
            case cls():  # type: ignore
                res.add(curr_key)
            case dict():
                res |= get_keys_containing_class(v, cls, curr_key)
            case list():
                for i, item in enumerate(v):
                    indexed_keypath = f"{curr_key}[{i}]"
                    if isinstance(item, cls):
                        res.add(indexed_keypath)
                    elif isinstance(item, dict):
                        res |= get_keys_containing_class(item, cls, indexed_keypath)
    return res


def drop_keys(source: dict[str, Any], keys_to_drop: Iterable[str]) -> dict[str, Any]:
    """
    Returns the dictionary with the requested keys set to `None`.

    If a key is a duplicate, then lookup fails so that key is skipped.

    DROP values are checked and handled here.
    """
    res = source
    seen_keys = set()
    for key in keys_to_drop:
        curr_keypath = get_tokenized_keypath(key)
        if curr_keypath not in seen_keys:
            if v := _nested_get(res, key):
                # Check if value has a DROP object
                if isinstance(v, DROP):
                    # If "out of bounds", raise an error
                    if v.value > 0 or -1 * v.value > len(curr_keypath):
                        raise RuntimeError(f"Error: DROP level {v} at {key} is invalid")
                    curr_keypath = curr_keypath[: v.value]  # type: ignore
                    # Handle case for dropping entire object
                    if len(curr_keypath) == 0:
                        return dict()
                if updated := _nested_set(res, curr_keypath, None):  # type: ignore
                    res = updated
                seen_keys.add(curr_keypath)
        else:
            seen_keys.add(curr_keypath)
    return res


def impute_enum_values(source: dict[str, Any], keys_to_impute: set[str]) -> dict[str, Any]:
    """
    Returns the dictionary with the Enum values set to their corresponding `.value`
    """
    res = source
    for key in keys_to_impute:
        curr_val = _nested_get(res, key)
        if isinstance(curr_val, KEEP):
            literal_val = curr_val.value
            res = _nested_set(res, get_tokenized_keypath(key), literal_val)  # type: ignore
    return res


def default_dsl(
    source: dict[str, Any] | list[Any],
    key: str,
    default: Any,
    override_key: tuple[GetDslTreeResults, ...] | None = None,
) -> Any:
    """
    Specifies a DSL (domain-specific language) to use when running `get`

    Uses the result from `get_tokenized_keypath` which runs parsimonious (a PEG grammar processor)
    """
    # Get results from DSL tree processing
    parsed_key = override_key if override_key else get_tokenized_keypath(key)
    # Process the results from the DSL tree processing
    res = source
    PLACEHOLDER_KEY = "NOT_USED"

    for i, item in enumerate(parsed_key):
        try:
            match item:
                case tuple():
                    # For each item in the tuple, re-run the DSL with tuple prefix
                    remaining_tup = parsed_key[i + 1 :]
                    # Handle base case (i.e. don't call further if no remaining needed)
                    if remaining_tup:
                        return tuple(
                            default_dsl(res[tup_key], PLACEHOLDER_KEY, default, remaining_tup)  # type: ignore
                            for tup_key in item
                        )
                    else:
                        return tuple(default_dsl(res, tup_key, default) for tup_key in item)
                case list():
                    remaining_key = tuple(parsed_key[i + 1 :])
                    # Use info if present, otherwise operate on entire list
                    if len(item) > 0:
                        if len(item) == 2:
                            next_list = res[item[0]]  # type: ignore
                            list_slice = item[1]
                        elif len(item) == 1:
                            next_list = res
                            list_slice = item[0]
                        else:
                            raise RuntimeError(
                                "Unexpected non-zero size when processing list operation"
                            )
                    else:
                        next_list = res
                        list_slice = slice(None)  # type: ignore
                    # Handle base case (i.e. don't call further if no remaining needed)
                    if remaining_key:
                        return [
                            default_dsl(next_item, PLACEHOLDER_KEY, None, remaining_key)
                            for next_item in next_list[list_slice]
                        ]
                    else:
                        return next_list[list_slice]
                case str():
                    # If at this point we have a `.`-delimited str, then assume it should be indexed-into
                    if "." in item:
                        for p in item.split("."):
                            res = res[p]  # type: ignore
                    else:
                        res = res[item]  # type: ignore
                case _:
                    # Keep indexing when it's index-able
                    res = res[item]  # type: ignore
        except (KeyError, IndexError):
            return default
        if res is None:
            break
    return res


class GetDSLVisitor(NodeVisitor):
    """
    Generates tree structure which is handled in `get_tokenized_keypath`

    The `KEEP` enum is used to signify the structure has semantic purpose and shouldn't be unnested
    """

    # === Top-level ===
    def visit_get_expr(
        self, node: Node, visited_children: Sequence[Any]
    ) -> list[GetDslTreeResults]:
        """Entrypoint: handles full expression like 'a[0].b[*].c'"""
        # Keeping original nesting of `vistied_children` (structure depends on `dsl.peg` definition)
        curr_expr: list[GetDslTreeResults] = []
        for child in visited_children:
            # Ignore things that resolve to `None`
            if child is None:
                continue
            # If it's wrapped in `KEEP`, then expect it to have semantic meaning (e.g. list unwrap)
            elif isinstance(child, KEEP):
                curr_expr.append(child.value)
            # For remaining tree branches, get flattened version
            elif isinstance(child, (tuple, list)):
                curr_expr.extend(c for c in flatten_sequence(child))
            # Return the value as-is
            else:
                curr_expr.append(child)
        return curr_expr

    # === Actionable Units ===
    def visit_single(self, node: Node, visited_children: Sequence[Any]) -> tuple:
        """Handle single key expressions like 'a[0]'"""
        return tuple(visited_children)

    def visit_list_op(self, node: Node, visited_children: Sequence[Any]) -> KEEP:
        """Handles expression meant to be applied on a list, e.g. `a[*]` or `[:1]`"""
        return KEEP(list(v for v in visited_children if v))

    def visit_tuple(self, node: Node, visited_children: Sequence[Any]) -> KEEP:
        """Handle tuple expressions like '(a,b,c)'"""
        res_items: tuple[list[str]] = tuple(flatten_sequence(visited_children))
        return KEEP(tuple(str(v) for v in res_items))

    # === Intermediate Representation ===
    def visit_single_index(self, node: Node, visited_children: Sequence[Any]) -> Any:
        """Handle index expressions like '[0]'"""
        # Just skip the brackets
        return visited_children[1]

    def visit_multi_index(self, node: Node, visited_children: Sequence[Any]) -> Any:
        """Handles index expressions '[*]' and slices like '[1:]'"""
        # Also just skip the brackets
        return visited_children[1]

    def visit_slice(self, node: Node, visited_children: Sequence[Any]) -> slice:
        """Handle slice notation like '[1:10]' or '[:]'"""
        assert len(visited_children) == 3  # Expect a '' node if missing
        # Skip the colon in the middle
        start = visited_children[0]
        stop = visited_children[-1]
        return slice(start, stop)

    def visit_nested_expr(self, node: Node, visited_children: Sequence[Any]) -> str:
        """Same as `get_expr` except serialize as a str"""
        # NOTE: consider just calling `visit_get_expr`, though this way easier to debug...
        # TODO: Have this serialize in better way
        #       (e.g. it'll cast everything to str and lose info for super nested case)
        curr_expr: list[GetDslTreeResults] = []
        for child in visited_children:
            # Ignore things that resolve to `None`
            if child is None:
                continue
            # If it's wrapped in `KEEP`, then expect it to have semantic meaning (e.g. list unwrap)
            elif isinstance(child, KEEP):
                curr_expr.append(child.value)
            # For remaining tree branches, get flattened version
            elif isinstance(child, (tuple, list)):
                curr_expr.extend(c for c in flatten_sequence(child))
            # Return the value as-is
            else:
                curr_expr.append(child)
        return ".".join(curr_expr)  # type: ignore

    # === Primitives / Lexemes (non-ignored) ===
    def visit_name(self, node: Node, visited_children: Sequence[Any]) -> str:
        """Handle identifiers like 'a', 'b', 'c'"""
        return node.text

    def visit_number(self, node: Node, visited_children: Sequence[Any]) -> int:
        """Handle numbers like '0', '-1'"""
        return int(node.text)

    # === ... everything else ===
    def generic_visit(
        self, node: Node, visited_children: Sequence[Any]
    ) -> Sequence[Any] | Any | None:
        """Default handler for unspecified rules"""
        # Generic behavior: return either
        #   1) multiple remaining child nodes
        #   2) a single remaining child node
        #   3) `None` if there's no children
        if len(visited_children) > 1:
            return visited_children
        elif len(visited_children) == 1:
            return visited_children[0]
        else:
            return None


def get_tokenized_keypath(key: str) -> tuple[GetDslTreeResults]:
    """
    Returns a keypath with str, ints and slices separated. Prefer tuples so it is hashable.

    E.g.: "a[0].b[-1].c" -> ("a", 0, "b", -1, "c")
         "a[1:3]" -> ("a", slice(1,3))

    Grammar is defined in `dicts/dsl.peg`
    """
    parsed_tree = GET_DSL_GRAMMAR.parse(key.replace(" ", ""))
    res = GetDSLVisitor().visit(parsed_tree)
    return tuple(res)


"""
Internal functions
"""


def _nested_get(source: dict[str, Any] | list[Any], key: str | Any, default: Any = None) -> Any:
    """
    Expects `.`-delimited string and tries to get the item in the dict.

    See `dsl.peg` for the formal grammar definition
    """
    # Try performing the DSL
    res = default_dsl(source, key, default)

    # DSL-independent cleanup
    if isinstance(res, list):
        res = [r if r is not None else default for r in res]
    elif res is None:
        res = default

    return res


def _nested_set(
    source: dict[str, Any], tokenized_key_list: Sequence[str | int], target: Any
) -> dict[str, Any] | None:
    """
    Returns a copy of source with the replace if successful, else None.
    """
    res: Any = source
    try:
        for k in tokenized_key_list[:-1]:
            res = res[k]
        res[tokenized_key_list[-1]] = target
    except IndexError:
        return None
    return source
