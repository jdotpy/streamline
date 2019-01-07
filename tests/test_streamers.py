from streamline import streamers
from streamline.core import static_pipe, sync_exec
from streamline.entries import entry_wrap, entry_unwrap, Entry

import asyncio
import re


def do_streamer_test(streamer, inputs, expected_outputs=None, options=None, wrap=True):
    if wrap:
        entries = entry_wrap(inputs)
    else:
        entries = inputs

    # Initialize streamer and queue up task
    future = static_pipe(streamer, entries)
    result = sync_exec(future)

    outputs = entry_unwrap(result)
    if expected_outputs:
        assert outputs == expected_outputs
    return outputs

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

    # Test for stupid things
    do_streamer_test(
        streamers.ExtractionStreamer(selector='bar[0].doodle').stream,
        [{'foo': 1}, {'foo': 2}],
        [None,None],
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
        streamers.ValueBreakdown(append_summary=True).stream,
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

def test_input_headers():
    a = Entry('a', index=0)
    a.value = 1
    b = Entry('b', index=1)
    b.value = {'label': 'second entry'}
    c = Entry('c', index=2)
    c.value = set(['third entry'])

    do_streamer_test(
        streamers.InputHeaders().stream,
        [a, b, c],
        [
            'a: 1',
            'b: {"label": "second entry"}',
            'c: {\'third entry\'}',
        ],
        wrap=False,
    )


    # With indexes
    a = Entry('a', index=0)
    a.value = 1
    b = Entry('b', index=1)
    b.value = {'label': 'second entry'}
    c = Entry('c', index=2)
    c.value = set(['third entry'])
    do_streamer_test(
        streamers.InputHeaders(indexes=True).stream,
        [a, b, c],
        [
            '[0] a: 1',
            '[1] b: {"label": "second entry"}',
            '[2] c: {\'third entry\'}',
        ],
        wrap=False,
    )

def test_error_values():
    a = Entry('a', error_value='custom_error_value')
    b = Entry('b', error_value='custom_error_value')
    c = Entry('c', error_value='custom_error_value')
    try:
        raise ValueError('Failure for a')
    except Exception as e:
        a.error(e)

    b_error = ValueError('Failure for b')
    b.error(b_error)
    outputs = do_streamer_test(streamers.error_values, [a, b, c], wrap=False)
    assert re.match('^ValueError: Failure for a.*File.*test_streamers.py.*', outputs[0], re.S +re.MULTILINE) is not None
    assert outputs[1] == b_error
    assert outputs[2] == 'c'

def test_buffer():
    do_streamer_test(
        streamers.StreamingBuffer().stream,
        [1,2,3,4,5],
        [1,2,3,4,5],
    )

def test_strip():
    # Defaults
    do_streamer_test(
        streamers.StripWhitespace().stream,
        [1,' 2 ','',' ', '5'],
        [1,'2','5'],
    )
    # Keep Blanks
    do_streamer_test(
        streamers.StripWhitespace(keep_blank=True).stream,
        [1,' 2 ','',' ', '5'],
        [1,'2','','','5'],
    )

def test_head():
    # Defaults
    do_streamer_test(
        streamers.HeadStreamer().stream,
        [1,2,3,4,5],
        [1],
    )

    # With Count
    do_streamer_test(
        streamers.HeadStreamer(count=4).stream,
        [1,2,3,4,5],
        [1,2,3,4],
    )
