<?php declare(strict_types=1);

namespace Tests\Kirameki\Redis\Adapters;

use Kirameki\Core\Exceptions\InvalidConfigException;
use Kirameki\Redis\Adapters\PhpRedisAdapter;
use Kirameki\Redis\Config\PhpRedisConfig;
use Kirameki\Redis\Exceptions\ConnectionException;
use Tests\Kirameki\Redis\TestCase;

final class PhpRedisAdapterTest extends TestCase
{
    public function test_connect__should_fail_when_neither_host_or_socket_is_set(): void
    {
        $this->expectException(InvalidConfigException::class);
        $this->expectExceptionMessage('Either host or socket must be provided.');
        $config = new PhpRedisConfig(host: null, socket: null);
        $adapter = new PhpRedisAdapter($config);
        $adapter->connect();
    }

    public function test_connect__should_fail_when_both_host_and_socket_is_set(): void
    {
        $this->expectException(InvalidConfigException::class);
        $this->expectExceptionMessage('Host and socket cannot be used together.');
        $config = new PhpRedisConfig(host: 'redis', socket: '/run/redis.sock');
        $adapter = new PhpRedisAdapter($config);
        $adapter->connect();
    }

    public function test_connect__try_with_socket(): void
    {
        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('No such file or directory');
        $config = new PhpRedisConfig(socket: '/run/redis.sock');
        $adapter = new PhpRedisAdapter($config);
        $adapter->connect();
    }

    public function test_rawCommand(): void
    {
        $adapter = new PhpRedisAdapter(new PhpRedisConfig('redis'));
        $adapter->connect();
        $result = $adapter->rawCommand('PING', []);
        $this->assertEquals('PONG', $result);
    }
}
