<?php declare(strict_types=1);

namespace Kirameki\Redis\Adapters;

use Kirameki\Core\Exceptions\InvalidConfigException;
use Kirameki\Redis\Config\ExtensionConfig;
use Kirameki\Redis\Exceptions\CommandException;
use Kirameki\Redis\Exceptions\ConnectionException;
use Kirameki\Redis\Exceptions\RedisException;
use Generator;
use Override;
use Redis;
use RedisException as PhpRedisException;
use function array_filter;
use function array_map;
use function iterator_to_array;
use function substr;

/**
 * @extends Adapter<ExtensionConfig>
 */
class ExtensionAdapter extends Adapter
{
    protected ?Redis $client = null;

    /**
     * @return Redis
     */
    protected function getClient(): Redis
    {
        return $this->client ??= $this->createClient(...$this->getClientArgs());
    }

    /**
     * @return Generator<int, Redis>
     */
    protected function getClientNodes(): Generator
    {
        yield $this->getClient();
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function connect(): static
    {
        $this->getClient();
        return $this;
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function disconnect(): bool
    {
        $result = (bool) $this->client?->close();
        $this->client = null;
        return $result;
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function isConnected(): bool
    {
        return $this->client !== null;
    }

    /**
     * @return array{ host: string, port: int, persistent: bool }
     */
    protected function getClientArgs(): array
    {
        $config = $this->config;
        $host = $config->host;
        $port = $config->port ?? 6379;
        $socket = $config->socket;
        if ($host === null && $socket === null) {
            throw new InvalidConfigException('Either host or socket must be defined.');
        }
        if ($host !== null && $socket !== null) {
            throw new InvalidConfigException('Host and socket cannot be used together.');
        }
        if ($host === null) {
            $host = $socket;
            $port = -1;
        }
        return [
            'host' => $host,
            'port' => $port,
            'persistent' => $config->persistent,
        ];
    }

    /**
     * @param string $host
     * @param int $port
     * @param bool $persistent
     * @return Redis
     */
    protected function createClient(string $host, int $port, bool $persistent = false): Redis
    {
        $client = new Redis();
        $config = $this->config;

        try {
            $connectTimeoutSeconds = $config->connectTimeoutSeconds ?? 0.0;
            $readTimeoutSeconds = $config->readTimeoutSeconds ?? 0.0;

            $persistent
                ? $client->pconnect($host, $port, $connectTimeoutSeconds, null, 0, $readTimeoutSeconds)
                : $client->connect($host, $port, $connectTimeoutSeconds, null, 0, $readTimeoutSeconds);

            $client->setOption(Redis::OPT_PREFIX, $config->prefix);
            $client->setOption(Redis::OPT_SCAN, Redis::SCAN_PREFIX);
            $client->setOption(Redis::OPT_SCAN, Redis::SCAN_RETRY);
            $client->setOption(Redis::OPT_SERIALIZER, $config->serializer);
            $client->setOption(Redis::OPT_TCP_KEEPALIVE, true);

            if ($config->username !== null && $config->password !== null) {
                $credentials = ['user' => $config->username, 'pass' => $config->password];
                $client->auth(array_filter($credentials, fn($v) => $v !== null));
            }

            if ($config->database !== null) {
                $client->select($config->database);
            }
        }
        catch (PhpRedisException $e) {
            $this->throwAs(ConnectionException::class, $e);
        }

        return $client;
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function command(string $name, array $args): mixed
    {
        $client = $this->getClient();

        try {
            $result = $client->$name(...$args);
        }
        catch (PhpRedisException $e) {
            $this->throwAs(CommandException::class, $e);
        }

        if ($err = $client->getLastError()) {
            $client->clearLastError();
            throw new CommandException($err);
        }

        return $result;
    }

    /**
     * @inheritDoc
     */
    public function dbSize(): int
    {
        try {
            $nodes = iterator_to_array($this->getClientNodes());
            $sizes = array_map(static fn(Redis $n): int => $n->dbSize(), $nodes);
            return array_sum($sizes);
        }
        catch (PhpRedisException $e) {
            $this->throwAs(CommandException::class, $e);
        }
    }

    /**
     * @param string $pattern
     * @param int $count
     * @param bool $prefixed
     * @return Generator<int, string>
     */
    public function scan(string $pattern = '*', int $count = 10_000, bool $prefixed = false): Generator
    {
        $prefixLength = strlen($this->config->prefix);

        try {
            foreach ($this->getClientNodes() as $client) {
                $cursor = null;
                do {
                    foreach ($client->scan($cursor, $pattern, $count) ?: [] as $key) {
                        yield $prefixed
                            ? $key
                            : substr($key, $prefixLength);
                    }
                } while ($cursor > 0);
            }
        }
        catch (PhpRedisException $e) {
            $this->throwAs(CommandException::class, $e);
        }
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
}
