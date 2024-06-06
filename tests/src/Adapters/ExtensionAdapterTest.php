<?php declare(strict_types=1);

namespace Tests\Kirameki\Redis\Adapters;

use Kirameki\Core\Exceptions\InvalidConfigException;
use Kirameki\Redis\Adapters\ExtensionAdapter;
use Kirameki\Redis\Config\ExtensionConfig;
use Kirameki\Redis\Exceptions\ConnectionException;
use Tests\Kirameki\Redis\TestCase;

final class ExtensionAdapterTest extends TestCase
{
    public function test_connect__should_fail_when_neither_host_or_socket_is_set(): void
    {
        $this->expectException(InvalidConfigException::class);
        $this->expectExceptionMessage('Either host or socket must be provided.');
        $config = new ExtensionConfig(host: null, socket: null);
        $adapter = new ExtensionAdapter($config);
        $adapter->connect();
    }

    public function test_connect__should_fail_when_both_host_and_socket_is_set(): void
    {
        $this->expectException(InvalidConfigException::class);
        $this->expectExceptionMessage('Host and socket cannot be used together.');
        $config = new ExtensionConfig(host: 'redis', socket: '/run/redis.sock');
        $adapter = new ExtensionAdapter($config);
        $adapter->connect();
    }

    public function test_connect__try_with_socket(): void
    {
        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('No such file or directory');
        $config = new ExtensionConfig(socket: '/run/redis.sock');
        $adapter = new ExtensionAdapter($config);
        $adapter->connect();
    }
}
