import argparse

from .utils import import_obj, inject_module

async def _get_client_keys():
    client = await asyncssh.connect_agent()
    keys = await client.get_keys()
    return keys

class ScpHandler():
    def __init__(self, source=None, target=None):
        inject_module('asyncssh', globals())
        self.source = source
        self.target = target
        self.client_keys = None

    @classmethod
    def parse_args(cls, parser):
        parser.add_argument(
            'source',
            nargs='?',
            help='File copy source'
        )
        parser.add_argument(
            'target',
            nargs='?',
            help='File copy target'
        )

    async def handle(self, entry):
        if not self.client_keys:
            self.client_keys = await _get_client_keys()

        source = self.source.format(entry=entry)
        target = self.target.format(entry=entry)
        async with asyncssh.connect(entry, known_hosts=None, client_keys=self.client_keys) as conn:
            if self.source.startswith('{entry}:'):
                source = (conn, source.split(':', 1)[1])
            if self.target.startswith('{entry}:'):
                target = (conn, target.split(':', 1)[1])
            await asyncssh.scp(source, target)
        return {
            'success': True,
            'source': self.source.format(entry=entry),
            'target': self.target.format(entry=entry),
        }

class ShellHandler():
    def __init__(self, command=None):
        self.command = command

    @classmethod
    def parse_args(cls, parser):
        parser.add_argument(
            'command',
            nargs='?',
            default='echo "{entry}"',
            help='Subprocess command to run'
        )

    async def handle(self, entry):
        command = self.command.format(entry=shlex.quote(entry))
        subprocess = await asyncio.create_subprocess_shell(
            command,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        stdout, stderr = await subprocess.communicate()
        return {
            'stdout': stdout.decode('utf-8'),
            'stderr': stderr.decode('utf-8'),
            'exit_code': subprocess.returncode,
        }

class SSHHandler():
    NO_SUDO = object()

    def __init__(self, command=None, continue_on_error=True, as_user=None):
        inject_module('asyncssh', globals())
        self.command = command
        self.continue_on_error = continue_on_error
        self.client_keys = None

        # Hackery around argparse's "empty" value being None for present arg
        # while the actual missing arg takes on the default defined in the 
        # `add_argument` function
        if as_user == self.NO_SUDO:
            self.as_user = None
        elif not as_user:
            self.as_user = 'root'
        else:
            self.as_user = as_user

    @classmethod
    def parse_args(cls, parser):
        parser.add_argument(
            'command',
            nargs='?',
            help='Command to run'
        )
        parser.add_argument(
            '--continue-on-error',
            action='store_true',
            default=False,
            help='Continue executing commands if non-success exit status is given',
        )
        parser.add_argument(
            '--sudo',
            nargs='?',
            default=cls.NO_SUDO,
            dest='as_user',
            help='Sudo as user'
        )

    async def handle(self, entry):
        if not self.client_keys:
            self.client_keys = await _get_client_keys()

        command_results = []
        commands = [self.command]
        host = entry.strip()
        async with asyncssh.connect(host, known_hosts=None, client_keys=self.client_keys) as conn:
            for command in commands:
                if self.as_user:
                    command = 'sudo -u {} {}'.format(self.as_user, command)
                result = await conn.run(command)
                command_results.append({
                    'host': entry,
                    'command': command,
                    'exit_code': result.exit_status,
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                })
                if result.exit_status != 0 and not self.continue_on_error:
                    break

        success = all([r['exit_code'] == 0 for r in command_results])
        if len(commands) == 1:
            return { 'success': success, **command_results[0] }
        return {
            'success': success,
            'commands': command_results,
        }

class HTTPHandler():
    def __init__(self, url=None, method=None):
        self.url = url
        self.method = method
        inject_module('requests', globals())

    @classmethod
    def parse_args(cls, parser):
        parser.add_argument(
            'url',
            nargs='?',
            default='http://{entry}',
            help='URL of resource (defaults to http://{entry})'
        )
        parser.add_argument(
            '--method',
            default='GET',
            help='HTTP Method to use (GET/POST, etc)'
        )

    def handle(self, entry):
        url = self.url.format(entry=entry)
        response = requests.request(self.method, url)
        result = {
            'headers': dict(response.headers),
            'code': response.status_code,
        }
        if response.headers.get('content-type', None) == 'application/json':
            result['response'] = response.json()
        else:
            result['response'] = response.text
        return result

EXECUTORS = {
    'http': HTTPHandler,
    'ssh': SSHHandler,
    'shell': ShellHandler,
    'scp': ScpHandler,
}

def load_executor(path, options):
    if path is None:
        return None
    elif '.' in path:
        Executor = import_obj(path)
    else:
        Executor = EXECUTORS.get(path)
    executor_parser = argparse.ArgumentParser()
    if Executor and hasattr(Executor, 'parse_args'):
        Executor.parse_args(executor_parser)
        executor_args = executor_parser.parse_args(options)
        executor = Executor(**executor_args.__dict__).handle
    else:
        executor = Executor
    return executor

