import subprocess
import argparse
import asyncio
import shlex

from .utils import import_obj, inject_module, arg_help

async def _get_client_keys():
    client = await asyncssh.connect_agent()
    keys = await client.get_keys()
    return keys

@arg_help('Treat each value as a host to connect to. Copy a file to or from this host', example='"/tmp/file.txt" "{value}:/tmp/file.txt')
class ScpHandler():
    async_handler = True
    def __init__(self, source=None, target=None):
        inject_module('asyncssh', globals())
        self.source = source
        self.target = target
        self.client_keys = None

    @classmethod
    def args(cls, parser):
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

    async def handle(self, value):
        if not self.client_keys:
            self.client_keys = await _get_client_keys()

        source = self.source.format(value=value)
        target = self.target.format(value=value)
        async with asyncssh.connect(value, known_hosts=None, client_keys=self.client_keys) as conn:
            if self.source.startswith('{value}:'):
                source = (conn, source.split(':', 1)[1])
            if self.target.startswith('{value}:'):
                target = (conn, target.split(':', 1)[1])
            await asyncssh.scp(source, target)
        return {
            'success': True,
            'source': self.source.format(value=value),
            'target': self.target.format(value=value),
        }

@arg_help('Run a shell command for each value', example='"nc -zv {value} 22"')
class ShellHandler():
    async_handler = True
    def __init__(self, command=None):
        self.command = command

    @classmethod
    def args(cls, parser):
        parser.add_argument(
            'command',
            nargs='?',
            default='echo "{value}"',
            help='Subprocess command to run'
        )

    async def handle(self, value):
        command = self.command.format(value=shlex.quote(value))
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

@arg_help('Treat each value as a host to connect to. SSH in and run a command returning the output', example='"uptime"')
class SSHHandler():
    async_handler = True
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
    def args(cls, parser):
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

    async def handle(self, value):
        if not self.client_keys:
            self.client_keys = await _get_client_keys()

        command_results = []
        commands = [self.command]
        host = value.strip()
        async with asyncssh.connect(host, known_hosts=None, client_keys=self.client_keys) as conn:
            for command in commands:
                if self.as_user:
                    command = 'sudo -u {} {}'.format(self.as_user, command)
                result = await conn.run(command)
                command_results.append({
                    'host': value,
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

@arg_help('Use a template to execute an HTTP request for each value', example='"https://{value}/"')
class HTTPHandler():
    async_handler = True

    def __init__(self, url=None, method=None):
        self.url = url
        self.method = method
        inject_module('requests', globals())

    @classmethod
    def args(cls, parser):
        parser.add_argument(
            'url',
            nargs='?',
            default='https://{value}/',
            help='URL of resource (defaults to https://{value}/)'
        )
        parser.add_argument(
            '--method',
            default='GET',
            help='HTTP Method to use (GET/POST, etc)'
        )

    def handle(self, value):
        url = self.url.format(value=value)
        response = requests.request(self.method, url, timeout=(5, None))
        result = {
            'headers': dict(response.headers),
            'code': response.status_code,
        }
        if response.headers.get('content-type', None) == 'application/json':
            result['response'] = response.json()
        else:
            result['response'] = response.text
        return result

@arg_help('Sleep for each entry making no change to its value')
class SleepHandler():
    async_handler = True
    
    async def handle(self, value):
        try:
            time = float(value)
        except Exception as e:
            print(e)
            time = 1
        await asyncio.sleep(time)
        return value

EXECUTORS = {
    'http': HTTPHandler,
    'ssh': SSHHandler,
    'shell': ShellHandler,
    'scp': ScpHandler,
    'sleep': SleepHandler,
}
