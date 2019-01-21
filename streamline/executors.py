import subprocess
import argparse
import asyncio
import urllib3
import uuid
import shlex
import os

urllib3.disable_warnings()

from .utils import import_obj, inject_module, arg_help

async def get_client_keys():
    client = await asyncssh.connect_agent()
    keys = await client.get_keys()
    return keys

class BaseAsyncSSHHandler():
    async_handler = True

    def __init__(self, **options):
        inject_module('asyncssh', globals())
        self.options = options
        self.client_keys = None

        # Hook for other things
        self.initialize()

    def initialize(self):
        pass

    async def _get_client_keys(self):
        if self.client_keys:
            return self.client_keys
        elif hasattr(self, '_client_key_future'):
            await asyncio.wait([self._client_key_future])
            return self.client_keys

        self._client_key_future = asyncio.ensure_future(get_client_keys())
        client_keys = await self._client_key_future
        self.client_keys = client_keys
        return client_keys

    async def handle(self, value):
        client_keys = await self._get_client_keys()

        async with asyncssh.connect(value.strip(), known_hosts=None, client_keys=client_keys) as conn:
            return await self.handle_connection(conn, value)

@arg_help('Treat each value as a host to connect to. Copy a file to or from this host', example='"/tmp/file.txt" "{value}:/tmp/file.txt"')
class ScpHandler(BaseAsyncSSHHandler):
    async_handler = True

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

    async def handle_connection(self, conn, value):
        formatted_source = self.options['source'].format(value=value)
        formatted_target = self.options['target'].format(value=value)

        target = formatted_target
        source = formatted_source
        if self.options['source'].startswith('{value}:'):
            source = (conn, formatted_source.split(':', 1)[1])
        if self.options['target'].startswith('{value}:'):
            target = (conn, formatted_target.split(':', 1)[1])

        await asyncssh.scp(source, target)
        return {
            'success': True,
            'source': formatted_source,
            'target': formatted_target,
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
class SSHHandler(BaseAsyncSSHHandler):
    NO_SUDO = object()

    def initialize(self):
        self.command = self.options['command']
        as_user = self.options.get('as_user', None)

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
            '--sudo',
            nargs='?',
            default=cls.NO_SUDO,
            dest='as_user',
            help='Sudo as user'
        )

    async def handle_connection(self, conn, value):
        if self.as_user:
            command = 'sudo -u {} {}'.format(self.as_user, self.command)
        else:
            command = self.command
        response = await conn.run(command)
        result = {
            'host': value,
            'command': command,
            'exit_code': response.exit_status,
            'success': response.exit_status == 0,
            'stdout': response.stdout,
            'stderr': response.stderr,
        }
        return result

@arg_help('Copy a script to target machine and execute', example='~/dostuff.sh')
class SSHExecHandler(BaseAsyncSSHHandler):
    NO_SUDO = object()

    def initialize(self):
        self.script_source = self.options['script']
        self.script_target = '~/{}.script'.format(uuid.uuid4())

        as_user = self.options.get('as_user', None)

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
            'script',
            nargs='?',
            help='Script to run'
        )
        parser.add_argument(
            '--sudo',
            nargs='?',
            default=cls.NO_SUDO,
            dest='as_user',
            help='Sudo as user'
        )

    async def handle_connection(self, conn, value):
        await asyncssh.scp(self.script_source, (conn, self.script_target))
        await conn.run('chmod +x {}'.format(self.script_target))
        if self.as_user:
            command = 'sudo -u {} {}'.format(self.as_user, self.script_target)
        else:
            command = self.script_target
        response = await conn.run(command)
        await conn.run('rm {}'.format(self.script_target))
        result = {
            'host': value,
            'command': command,
            'exit_code': response.exit_status,
            'success': response.exit_status == 0,
            'stdout': response.stdout,
            'stderr': response.stderr,
        }
        return result

@arg_help('Use a template to execute an HTTP request for each value', example='"https://{value}/"')
class HTTPHandler():
    async_handler = True

    def __init__(self, url=None, method=None, auth=None, no_verify=False):
        self.url = url
        self.method = method
        inject_module('requests', globals())

        self.auth = None
        if auth:
            self.auth = tuple(auth.split(':'))
        elif 'STREAMLINE_HTTP_AUTH' in os.environ:
            self.auth = tuple(os.environ.get('STREAMLINE_HTTP_AUTH').split(':'))
        if self.auth and len(self.auth) != 2:
            raise ValueError('Incorrect auth value. Format is "user:password"')

        self.verify = not no_verify


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
        parser.add_argument(
            '--auth',
            help='HTTP authentication to use (e.g. user:password)'
        )
        parser.add_argument(
            '--no-verify',
            default=False,
            action='store_true',
            help='Disable SSL verification'
        )

    def handle(self, value):
        url = self.url.format(value=value)
        response = requests.request(
            self.method,
            url,
            timeout=(5, None),
            auth=self.auth,
            verify=self.verify,
        )
        result = {
            'headers': dict(response.headers),
            'code': response.status_code,
        }
        if response.headers.get('content-type', None) == 'application/json':
            result['response'] = response.json()
        else:
            result['response'] = response.text
        return result

@arg_help('Sleep for a second (or for {value} seconds) for each entry making no change to its value')
class SleepHandler():
    async_handler = True
    
    async def handle(self, value):
        try:
            time = float(value)
        except Exception as e:
            time = 1
        await asyncio.sleep(time)
        return value

EXECUTORS = {
    'http': HTTPHandler,
    'ssh': SSHHandler,
    'ssh_exec': SSHExecHandler,
    'shell': ShellHandler,
    'scp': ScpHandler,
    'sleep': SleepHandler,
}
