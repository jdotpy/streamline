import asyncio
import asyncssh
import pytest
import pytest_asyncio
from streamline.executors import SSHHandler
from unittest.mock import AsyncMock, patch


@pytest_asyncio.fixture
async def ssh_test_env(tmp_path):
    # 1. Generate server host key and write to disk
    host_key_path = tmp_path / "ssh_host_key"
    host_key = asyncssh.generate_private_key('ssh-ed25519')
    host_key.write_private_key(str(host_key_path))

    # 2. Generate CA private key
    ca_key = asyncssh.generate_private_key('ssh-ed25519')

    # 3. Generate User private key and write to disk
    user_key_path = tmp_path / "id_rsa"
    user_key = asyncssh.generate_private_key('ssh-ed25519')
    user_key.write_private_key(str(user_key_path))

    # 4. Sign the user's key with the CA key natively in Python
    user_cert = ca_key.generate_user_certificate(
        user_key,
        'test_key_id',
        principals=['testuser'],
        serial=1
    )

    # 5. Write out the certificate file
    cert_path = tmp_path / "id_rsa-cert.pub"
    user_cert.write_certificate(str(cert_path))

    # 6. Create authorized_client_keys containing the CA public key
    ca_pub_text = ca_key.export_public_key().decode('utf-8').strip()
    auth_keys_path = tmp_path / "authorized_keys"
    auth_keys_path.write_text(f"cert-authority {ca_pub_text}\n")

    # 7. Define a minimal SSHServer implementation for the test
    class TestServer(asyncssh.SSHServer):
        def connection_requested(self):
            return True

    async def handle_client(process):
        command = process.command or ""
        # Echo back command or expected output for assertions
        process.stdout.write(f"hello from test: {command}\n")
        process.exit(0)

    # 8. Start the embedded AsyncSSH server on a random local port
    server = await asyncssh.create_server(
        TestServer,
        host='127.0.0.1',
        port=0,
        server_host_keys=[str(host_key_path)],
        authorized_client_keys=str(auth_keys_path),
        process_factory=handle_client
    )

    # Extract the actual bound port assigned by the OS
    port = next(iter(server.sockets)).getsockname()[1]
    target_addr = f"127.0.0.1:{port}"

    yield target_addr, str(user_key_path), str(cert_path)

    # 9. Cleanup server
    server.close()
    await server.wait_closed()


@pytest.mark.asyncio
async def test_ssh_handler_with_certificate(ssh_test_env):
    target_addr, user_key, client_cert = ssh_test_env
    host, port_str = target_addr.split(":")

    # Instantiate your handler with the generated test credentials
    handler = SSHHandler(
        username="testuser",
        client_keys=[user_key],
        client_certs=[client_cert],
        command="echo 'hello from test'"
    )

    handler.connection_options['port'] = int(port_str)
    handler.connection_options['known_hosts'] = None  # Disable strict host key checking for local test

    # Run the handler against the embedded test server
    result = await handler.handle(host)

    assert result['success'] is True
    assert "hello from test" in result['stdout']


@pytest.mark.asyncio
async def test_ssh_handler_with_rsa_certificate(tmp_path):
    # 1. Generate RSA server host key
    host_key_path = tmp_path / "ssh_host_key"
    host_key = asyncssh.generate_private_key('ssh-rsa')
    host_key.write_private_key(str(host_key_path))

    # 2. Generate RSA CA and User keys
    ca_key = asyncssh.generate_private_key('ssh-rsa')
    user_key_path = tmp_path / "id_rsa"
    user_key = asyncssh.generate_private_key('ssh-rsa')
    user_key.write_private_key(str(user_key_path))

    # 3. Sign the user RSA key with the CA
    user_cert = ca_key.generate_user_certificate(
        user_key,
        'rsa_test_key_id',
        principals=['testuser'],
        serial=1
    )

    cert_path = tmp_path / "id_rsa-cert.pub"
    user_cert.write_certificate(str(cert_path))

    ca_pub_text = ca_key.export_public_key().decode('utf-8').strip()
    auth_keys_path = tmp_path / "authorized_keys"
    auth_keys_path.write_text(f"cert-authority {ca_pub_text}\n")

    class TestServer(asyncssh.SSHServer):
        def connection_requested(self):
            return True

    async def handle_client(process):
        process.stdout.write("rsa cert auth success\n")
        process.exit(0)

    server = await asyncssh.create_server(
        TestServer,
        host='127.0.0.1',
        port=0,
        server_host_keys=[str(host_key_path)],
        authorized_client_keys=str(auth_keys_path),
        signature_algs=['rsa-sha2-256', 'rsa-sha2-512'],
        process_factory=handle_client
    )
    port = next(iter(server.sockets)).getsockname()[1]

    # Run handler with RSA key + cert
    handler = SSHHandler(
        username="testuser",
        client_keys=[str(user_key_path)],
        client_certs=[str(cert_path)],
        command="id"
    )
    handler.connection_options['port'] = port
    handler.connection_options['known_hosts'] = None

    try:
        result = await handler.handle("127.0.0.1")
        assert result['success'] is True
        assert "rsa cert auth success" in result['stdout']
    finally:
        server.close()
        await server.wait_closed()



@pytest.mark.asyncio
async def test_ssh_handler_agent_certificate_pairing(tmp_path, monkeypatch):
    # Clear the real SSH_AUTH_SOCK so it doesn't leak into the test environment
    monkeypatch.delenv("SSH_AUTH_SOCK", raising=False)

    # 1. Generate a CA key and a user key/cert
    ca_key = asyncssh.generate_private_key('ssh-rsa')
    user_key = asyncssh.generate_private_key('ssh-rsa')
    
    user_cert = ca_key.generate_user_certificate(
        user_key,
        'agent_test_key_id',
        principals=['testuser'],
        serial=2
    )
    cert_path = tmp_path / "agent_id_rsa-cert.pub"
    user_cert.write_certificate(str(cert_path))

    # 2. Set the environment variable for the certificate path (testing the env var path)
    monkeypatch.setenv("SSH_CLIENT_CERT", str(cert_path))

    # 3. Mock asyncssh.connect_agent() to return our user_key inside a mock agent context manager
    mock_agent = AsyncMock()
    # agent.get_keys() should return a list containing our raw private key object
    mock_agent.get_keys.return_value = [user_key]

    # asyncssh.connect_agent() is an async context manager, so we mock its __aenter__
    mock_agent_ctx = AsyncMock()
    mock_agent_ctx.__aenter__.return_value = mock_agent