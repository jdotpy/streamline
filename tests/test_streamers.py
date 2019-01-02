from streamline import streamers
from streamline.core import static_pipe, sync_exec
from streamline.entries import entry_wrap, entry_unwrap

import asyncio


def do_streamer_test(streamer, inputs, expected_outputs, options=None):
    entries = entry_wrap(inputs)

    # Initialize streamer and queue up task
    future = static_pipe(streamer, entries)
    result = sync_exec(future)

    outputs = entry_unwrap(result)
    assert outputs == expected_outputs

def test_extraction_streamer():
    # Basic single-depth
    do_streamer_test(
        streamers.ExtractionStreamer(selector='foo').stream,
        [{'foo': 'bar'}, {'foo': 1}],
        ['bar', 1],
    )

    # Include array indexes
    do_streamer_test(
        streamers.ExtractionStreamer(selector='foo.numbers[2]').stream,
        [{'foo': {'numbers': [1, 2, 3]}}],
        [3],
    )
    
    # Extra separators should do nothing
    do_streamer_test(
        streamers.ExtractionStreamer(selector='foo.numbers.[2]').stream,
        [{'foo': {'numbers': [1, 2, 3]}}],
        [3],
    )

    # Allow for * index keys that recurse
    do_streamer_test(
        streamers.ExtractionStreamer(selector='foo.numbers[*]').stream,
        [{'foo': {'numbers': [1, 2, 3]}}],
        [[1,2,3]],
    )

    # Allow for * keys that recurse
    do_streamer_test(
        streamers.ExtractionStreamer(selector='foo.*[0]').stream,
        [{'foo': {'numbers': [1, 2, 3], 'other_numbers': [4,5,6]}}],
        [[1,4]],
    )

def test_py_exec_transform():
    # Expression
    do_streamer_test(
        streamers.PyExecTransform(code='value.title()').stream,
        ['hi', 'hello'],
        ['Hi', 'Hello'],
    )
    # Full statements
    do_streamer_test(
        streamers.PyExecTransform(
            statement=True,
            code="foo = value.title(); result=foo;",
        ).stream,
        ['hi', 'hello'],
        ['Hi', 'Hello'],
    )

def test_py_exec_filter():
    # Expression
    do_streamer_test(
        streamers.PyExecFilter(code='"foo" in value').stream,
        ['foobar', 'hello'],
        ['foobar'],
    )

def test_truth_filter():
    do_streamer_test(
        streamers.truthy,
        ['foobar', '', 0, 34, None, True],
        ['foobar', 34, True],
    )

def test_json_parser():
    do_streamer_test(
        streamers.json_parser,
        ['{"foo": "bar"}', '34', '[1,2]'],
        [{"foo": "bar"}, 34, [1,2]],
    )

async def example_async_executor(value):
    await asyncio.sleep(.1)
    return value + 1

def test_async_executor():
    do_streamer_test(
        streamers.AsyncExecutor(executor=example_async_executor, workers=5).stream,
        [1,2,3,4,5],
        [2,3,4,5,6],
    )

def test_split_lists():
    do_streamer_test(
        streamers.split_lists,
        [[1,2], ['a','b']],
        [1,2,'a','b'],
    )

def test_value_breakdown():
    # summary mode
    do_streamer_test(
        streamers.ValueBreakdown().stream,
        ['A', 'B', 'A', 'B', 'B', 'C', 'Z'],
        [
            {'value': 'A', 'count': 2},
            {'value': 'B', 'count': 3},
            {'value': 'C', 'count': 1},
            {'value': 'Z', 'count': 1},
        ],
    )

    # inputs mode
    do_streamer_test(
        streamers.ValueBreakdown(inputs=True).stream,
        ['A', 'B', 'A', 'B', 'B', 'C', 'Z'],
        [
            {'value': 'A', 'count': 2, 'inputs': ['A', 'A']},
            {'value': 'B', 'count': 3, 'inputs': ['B', 'B', 'B']},
            {'value': 'C', 'count': 1, 'inputs': ['C']},
            {'value': 'Z', 'count': 1, 'inputs': ['Z']},
        ],
    )

    # append mode
    do_streamer_test(
        streamers.ValueBreakdown(append=True).stream,
        ['A', 'B', 'A', 'B', 'B', 'C', 'Z'],
        [
            'A', 'B', 'A', 'B', 'B', 'C', 'Z',
            [
                {'value': 'A', 'count': 2},
                {'value': 'B', 'count': 3},
                {'value': 'C', 'count': 1},
                {'value': 'Z', 'count': 1},
            ],
        ],
    )
