<?php declare(strict_types=1);

namespace Kirameki\Redis\Adapters;

use Closure;
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
use function array_sum;
use function count;
use function dump;
use function iterator_to_array;
use function strlen;
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
        return $this->client ??= $this->createClient();
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
     * @return array{ string, int }
     */
    protected function getClientConnectInfo(): array
    {
        $config = $this->config;
        $host = $config->host;
        $port = $config->port ?? 6379;
        $socket = $config->socket;
        if ($host === null && $socket === null) {
            throw new InvalidConfigException('Either host or socket must be provided.');
        }
        if ($host !== null && $socket !== null) {
            throw new InvalidConfigException('Host and socket cannot be used together.');
        }
        if ($host === null) {
            $host = $socket;
            $port = -1;
        }
        return [$host, $port];
    }

    /**
     * @return Redis
     */
    protected function createClient(): Redis
    {
        try {
            $client = new Redis();
            $config = $this->config;
            [$host, $port] = $this->getClientConnectInfo();
            $connectTimeoutSeconds = $config->connectTimeoutSeconds ?? 0.0;
            $readTimeoutSeconds = $config->readTimeoutSeconds ?? 0.0;

            $config->persistent
                ? @$client->pconnect($host, $port, $connectTimeoutSeconds, null, 0, $readTimeoutSeconds)
                : @$client->connect($host, $port, $connectTimeoutSeconds, null, 0, $readTimeoutSeconds);

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
            return $client;
        }
        catch (PhpRedisException $e) {
            $this->throwAs(ConnectionException::class, $e);
        }
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function command(string $name, array $args): mixed
    {
        return $this->execCommand($name, $args, false);
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function rawCommand(string $name, array $args): mixed
    {
        return $this->execCommand($name, $args, true);
    }

    /**
     * @param string $name
     * @param list<mixed> $args
     * @param bool $raw
     * @return mixed
     */
    protected function execCommand(string $name, array $args, bool $raw): mixed
    {
        $client = $this->getClient();

        $result = $raw
            ? $this->withCatch(static fn() => $client->rawCommand($name, ...$args))
            : $this->withCatch(static fn() => $client->$name(...$args));

        if ($err = $client->getLastError()) {
            $client->clearLastError();
            throw new CommandException($err);
        }

        return $result;
    }

    /**
     * @template T
     * @param Closure(): T $callback
     * @return T
     */
    protected function withCatch(Closure $callback): mixed
    {
        try {
            return $callback();
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

    /**
     * @template T
     * @param Closure(Redis): T $callback
     * @return T
     */
    protected function run(Closure $callback): mixed
    {
        $client = $this->getClient();

        $result = $this->withCatch(static fn() => $callback($client));

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
        return $this->withCatch(function(): int {
            $nodes = iterator_to_array($this->getClientNodes());
            $sizes = array_map(static fn(Redis $n): int => $n->dbSize(), $nodes);
            return array_sum($sizes);
        });
    }

    /**
     * @inheritDoc
     */
    public function scan(string $pattern = '*', int $count = 10_000, bool $prefixed = false): Generator
    {
        return $this->withCatch(function() use ($pattern, $count, $prefixed): Generator {
            $prefixLength = strlen($this->config->prefix);
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
        });
    }

    # region CONNECTION ------------------------------------------------------------------------------------------------

    /**
     * @inheritDoc
     */
    public function clientId(): int
    {
        return $this->run(static fn(Redis $r) => $r->client('id'));
    }

    /**
     * @inheritDoc
     */
    public function clientInfo(): array
    {
        return $this->run(static fn(Redis $r) => $r->client('info'));
    }

    /**
     * @inheritDoc
     */
    public function clientKill(string $ipAddressAndPort): bool
    {
        return $this->run(static fn(Redis $r) => $r->client('kill', $ipAddressAndPort));
    }

    /**
     * @inheritDoc
     */
    public function clientList(): array
    {
        return $this->getClient()->client('list');
    }

    /**
     * @inheritDoc
     */
    public function clientGetname(): ?string
    {
        $result = $this->run(static fn(Redis $r) => $r->client('getname'));
        return $result !== false ? $result : null;
    }

    /**
     * @inheritDoc
     */
    public function clientSetname(string $name): void
    {
        $this->run(static fn(Redis $r) => $r->client('setname', $name));
    }

    /**
     * @inheritDoc
     */
    public function echo(string $message): string
    {
        return $this->run(static fn(Redis $r) => $r->echo($message));
    }

    /**
     * @inheritDoc
     */
    public function ping(): bool
    {
        return $this->run(static fn(Redis $r) => $r->ping());
    }

    # endregion CONNECTION ---------------------------------------------------------------------------------------------

    # region GENERIC ---------------------------------------------------------------------------------------------------

    /**
     * @inheritDoc
     */
    public function del(string ...$key): int
    {
        if (count($key) === 0) {
            return 0;
        }
        return $this->run(static fn(Redis $r) => $r->del(...$key));
    }

    /**
     * @inheritDoc
     */
    public function exists(string ...$key): int
    {
        if (count($key) === 0) {
            return 0;
        }
        return $this->run(static fn(Redis $r) => $r->exists(...$key));
    }

}
