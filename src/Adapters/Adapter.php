<?php declare(strict_types=1);

namespace Kirameki\Redis\Adapters;

use Generator;
use Kirameki\Redis\Config\ConnectionConfig;
use Kirameki\Redis\Exceptions\CommandException;
use Kirameki\Redis\Exceptions\ConnectionException;
use Kirameki\Redis\Exceptions\RedisException;
use Redis;
use RedisCluster;
use RedisException as PhpRedisException;
use function array_filter;
use function strlen;
use function substr;

/**
 * @template TConnectionConfig of ConnectionConfig
 */
abstract class Adapter
{
    /**
     * @var Redis|RedisCluster|null
     */
    protected ?object $redis;

    /**
     * @param TConnectionConfig $config
     */
    public function __construct(
        public readonly ConnectionConfig $config,
    )
    {
        $this->redis = null;
    }

    /**
     * @return bool
     */
    public function connect(): bool
    {
        $this->getConnectedClient();
        return true;
    }

    /**
     * @return bool
     */
    public function disconnect(): bool
    {
        return (bool) $this->redis?->close();
    }

    /**
     * @return $this
     */
    public function reconnect(): static
    {
        $this->disconnect();
        $this->connect();
        return $this;
    }

    /**
     * @return bool
     */
    public function isConnected(): bool
    {
        return $this->redis !== null;
    }

    /**
     * @return Redis|RedisCluster
     */
    abstract protected function getConnectedClient(): object;

    /**
     * @param string $host
     * @param int $port
     * @return Redis
     */
    protected function connectDirect(string $host, int $port, bool $persistent): Redis
    {
        $redis = new Redis();
        $config = $this->config;

        try {
            $connectTimeoutSeconds = $config->connectTimeoutSeconds ?? 0.0;
            $readTimeoutSeconds = $config->readTimeoutSeconds ?? 0.0;

            $persistent
                ? $redis->pconnect($host, $port, $connectTimeoutSeconds, null, 0, $readTimeoutSeconds)
                : $redis->connect($host, $port, $connectTimeoutSeconds, null, 0, $readTimeoutSeconds);

            $redis->setOption(Redis::OPT_PREFIX, $config->prefix);
            $redis->setOption(Redis::OPT_TCP_KEEPALIVE, true);
            $redis->setOption(Redis::OPT_SERIALIZER, Redis::SERIALIZER_IGBINARY);

            if ($config->username !== null && $config->password !== null) {
                $credentials = ['user' => $config->username, 'pass' => $config->password];
                $redis->auth(array_filter($credentials, fn($v) => $v !== null));
            }
        }
        catch (PhpRedisException $e) {
            $this->throwAs(ConnectionException::class, $e);
        }

        return $redis;
    }

    /**
     * @return list<Redis>
     */
    abstract public function connectToNodes(): array;

    /**
     * @param string $name
     * @param mixed ...$args
     * @return mixed
     */
    public function command(string $name, mixed ...$args): mixed
    {
        $redis = $this->getConnectedClient();

        try {
            $result = $redis->$name(...$args);
        }
        catch (PhpRedisException $e) {
            $this->throwAs(CommandException::class, $e);
        }

        if ($err = $redis->getLastError()) {
            $redis->clearLastError();
            throw new CommandException($err);
        }

        return $result;
    }

    /**
     * @param class-string<RedisException> $class
     * @param PhpRedisException $base
     * @return no-return
     */
    protected function throwAs(string $class, PhpRedisException $base): never
    {
        // Dig through exceptions to get to the root one that is not wrapped in RedisException
        // since wrapping it twice is pointless.
        $root = $base;
        while ($last = $root->getPrevious()) {
            $root = $last;
        }
        throw new $class($base->getMessage(), $base->getCode(), $root);
    }

    /**
     * @param string|null $pattern
     * @param int $count
     * @param bool $prefixed
     * @return Generator<int, string>
     */
    public function scan(string $pattern = null, int $count = 0, bool $prefixed = false): Generator
    {
        // If the prefix is defined, doing an empty scan will actually call scan with `"MATCH" "{prefix}"`
        // which does not return the expected result. To get the expected result, '*' needs to be appended.
        if ($pattern === null && $this->config->prefix !== '') {
            $pattern = '*';
        }

        foreach ($this->connectToNodes() as $node) {
            $prefixed
                ? $node->setOption(Redis::OPT_SCAN, Redis::SCAN_PREFIX)
                : $node->setOption(Redis::OPT_SCAN, Redis::SCAN_NOPREFIX);
            $cursor = null;
            do {
                $keys = $node->scan($cursor, $pattern, $count);
                if ($keys !== false) {
                    foreach ($keys as $key) {
                        yield $key;
                    }
                }
            }
            while($cursor > 0);
        }
    }
}
