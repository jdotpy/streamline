import shlex
import sys
import io

from streamline import cli

def noop():
    pass

class FakeIO():
    def __init__(self, stdin_text=''):
        self.fake_stdin = io.StringIO(stdin_text)
        self.fake_stdout = io.StringIO()
        self.fake_stderr = io.StringIO()

        # Make fake io close method do nothing so data isn't discarded
        self.fake_stdin.close = noop
        self.fake_stdout.close = noop
        self.fake_stderr.close = noop

        self.stdin = sys.stdin
        self.stdout = sys.stdout
        self.stderr = sys.stderr

    def __enter__(self):
        sys.stdin = self.fake_stdin
        sys.stdout = self.fake_stdout
        sys.stderr = self.fake_stderr

    def __exit__(self, *args, **kwargs):
        sys.stdin = self.stdin
        sys.stdout = self.stdout
        sys.stderr = self.stderr

    def read_all(self, attr):
        stream = getattr(self, 'fake_' + attr)
        stream.seek(0)
        return stream.read()

def do_cli_call(command, input_text, expected_output):
    args = shlex.split(command)[1:]
    fake_io = FakeIO(input_text)
    with fake_io:
        cli.streamline_command(args)
    output_text = fake_io.read_all('stdout')
    assert output_text == expected_output

def test_noop_e2e():
    # A call with no arguments should translate the input without having an effect
    do_cli_call('streamline', "Foo\nBar", "Foo\nBar")
    
def test_ae_e2e():
    do_cli_call('streamline -m sleep', "Foo\nBar", "Foo\nBar")

def test_headers():
    do_cli_call('streamline --python "value.upper()" --headers', "Foo\nBar", "Foo: FOO\nBar: BAR")
