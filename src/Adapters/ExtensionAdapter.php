<?php declare(strict_types=1);

namespace Kirameki\Redis\Adapters;

use Closure;
use DateTimeInterface;
use Exception;
use Kirameki\Core\Exceptions\InvalidConfigException;
use Kirameki\Core\Exceptions\LogicException;
use Kirameki\Redis\Config\ExtensionConfig;
use Kirameki\Redis\Exceptions\CommandException;
use Kirameki\Redis\Exceptions\ConnectionException;
use Generator;
use Kirameki\Redis\Exceptions\RedisException;
use Kirameki\Redis\Options\SetMode;
use Kirameki\Redis\Options\TtlMode;
use Kirameki\Redis\Options\Type;
use Kirameki\Redis\Options\XtrimMode;
use Override;
use Redis;
use RedisException as PhpRedisException;
use function array_filter;
use function array_map;
use function array_sum;
use function count;
use function iterator_to_array;
use function strlen;
use function substr;

/**
 * @implements Adapter<ExtensionConfig>
 */
class ExtensionAdapter implements Adapter
{
    /**
     * @param ExtensionConfig $config
     * @param Redis|null $client
     */
    public function __construct(
        protected ExtensionConfig $config,
        protected ?Redis $client = null,
    )
    {
    }

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
     * @param Closure(Redis): mixed $callback
     * @return mixed
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
     * @param Exception $base
     * @return no-return
     */
    protected function throwAs(string $class, Exception $base): never
    {
        // Dig through exceptions to get to the root one that is not wrapped in RedisException
        // since wrapping it twice is pointless.
        $root = $base;
        while ($last = $root->getPrevious()) {
            $root = $last;
        }
        throw new $class($base->getMessage(), $base->getCode(), $root);
    }

    # region CONNECTION ------------------------------------------------------------------------------------------------

    /**
     * @inheritDoc
     */
    #[Override]
    public function clientId(): int
    {
        return $this->run(static fn(Redis $r) => $r->client('id'));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function clientInfo(): array
    {
        return $this->run(static fn(Redis $r) => $r->client('info'));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function clientKill(string $ipAddressAndPort): bool
    {
        return $this->run(static fn(Redis $r) => $r->client('kill', $ipAddressAndPort));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function clientList(): array
    {
        return $this->getClient()->client('list');
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function clientGetname(): ?string
    {
        $result = $this->run(static fn(Redis $r) => $r->client('getname'));
        return $result !== false ? $result : null;
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function clientSetname(string $name): void
    {
        $this->run(static fn(Redis $r) => $r->client('setname', $name));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function echo(string $message): string
    {
        return $this->run(static fn(Redis $r) => $r->echo($message));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function ping(): bool
    {
        return $this->run(static fn(Redis $r) => $r->ping());
    }

    # endregion CONNECTION ---------------------------------------------------------------------------------------------

    # region GENERIC ---------------------------------------------------------------------------------------------------

    /**
     * @inheritDoc
     */
    #[Override]
    public function rawCommand(string $name, array $args): mixed
    {
        return $this->run(static fn(Redis $r) => $r->rawCommand($name, ...$args));
    }

    /**
     * @inheritDoc
     */
    #[Override]
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
    #[Override]
    public function exists(string ...$key): int
    {
        if (count($key) === 0) {
            return 0;
        }
        return $this->run(static fn(Redis $r) => $r->exists(...$key));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function expireTime(string $key): int|false|null
    {
        $result = $this->run(static fn(Redis $r) => $r->expiretime($key));
        return match($result) {
            -2 => false,
            -1 => null,
            default => $result,
        };
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function pExpireTime(string $key): int|false|null
    {
        $result = $this->run(static fn(Redis $r) => $r->pexpiretime($key));
        return match($result) {
            -2 => false,
            -1 => null,
            default => $result,
        };
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function persist(string $key): bool
    {
        return $this->run(static fn(Redis $r) => $r->persist($key));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function expire(string $key, int $seconds, ?TtlMode $mode = null): bool
    {
        return $this->run(static fn(Redis $r) => $r->expire($key, $seconds, $mode?->value));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function pExpire(string $key, int $milliseconds, ?TtlMode $mode = null): bool
    {
        return $this->run(static fn(Redis $r) => $r->pExpire($key, $milliseconds, $mode?->value));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function expireAt(string $key, DateTimeInterface $time, ?TtlMode $mode = null): bool
    {
        return $this->run(static fn(Redis $r) => $r->expireAt($key, $time->getTimestamp(), $mode?->value));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function pExpireAt(string $key, DateTimeInterface $time, ?TtlMode $mode = null): bool
    {
        return $this->run(static fn(Redis $r) => $r->pExpireAt($key, $time->getTimestamp() * 1000, $mode?->value));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function randomKey(): ?string
    {
        $result = $this->run(static fn(Redis $r) => $r->randomKey());
        return $result !== false ? $result : null;
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function rename(string $srcKey, string $dstKey): bool
    {
        return $this->run(static fn(Redis $r) => $r->rename($srcKey, $dstKey));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function renameNx(string $srcKey, string $dstKey): bool
    {
        return $this->run(static fn(Redis $r) => $r->renameNx($srcKey, $dstKey));
    }

    /**
     * @inheritDoc
     */
    #[Override]
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

    /**
     * @inheritDoc
     */
    #[Override]
    public function ttl(string $key): int|false|null
    {
        $result = $this->run(static fn(Redis $r) => $r->ttl($key));
        return match($result) {
            -2 => false,
            -1 => null,
            default => $result,
        };
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function pTtl(string $key): int|false|null
    {
        $result = $this->run(static fn(Redis $r) => $r->pttl($key));
        return match($result) {
            -2 => false,
            -1 => null,
            default => $result,
        };
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function type(string $key): Type
    {
        $type = $this->run(static fn(Redis $r) => $r->type($key));
        return match ($type) {
            Redis::REDIS_NOT_FOUND => Type::None,
            Redis::REDIS_STRING => Type::String,
            Redis::REDIS_LIST => Type::List,
            Redis::REDIS_SET => Type::Set,
            Redis::REDIS_ZSET => Type::ZSet,
            Redis::REDIS_HASH => Type::Hash,
            Redis::REDIS_STREAM => Type::Stream,
            default => throw new LogicException("Unknown Type: $type"),
        };
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function unlink(string ...$key): int
    {
        return $this->run(static fn(Redis $r) => $r->unlink(...$key));
    }

    # endregion GENERIC ------------------------------------------------------------------------------------------------

    # region LIST ------------------------------------------------------------------------------------------------------

    /**
     * @inheritDoc
     */
    #[Override]
    public function blPop(iterable $keys, int|float $timeout = 0): ?array
    {
        $keys = iterator_to_array($keys);

        /** @var array{ 0?: string, 1?: mixed } $result */
        $result = $this->run(static fn(Redis $r) => $r->blPop($keys, $timeout));

        return (count($result) > 0)
            ? [$result[0] => $result[1]]
            : null;
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function lIndex(string $key, int $index): mixed
    {
        return $this->run(static fn(Redis $r) => $r->lIndex($key, $index));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function lPush(string $key, mixed ...$value): int
    {
        return $this->run(static fn(Redis $r) => $r->lPush($key, ...$value));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function rPush(string $key, mixed ...$value): int
    {
        return $this->run(static fn(Redis $r) => $r->rPush($key, ...$value));
    }

    # endregion LIST ---------------------------------------------------------------------------------------------------

    # region SCRIPT ----------------------------------------------------------------------------------------------------

    /**
     * @inheritDoc
     */
    #[Override]
    public function eval(string $script, int $numKeys = 0, int|string ...$arg): mixed
    {
        return $this->run(static fn(Redis $r) => $r->eval($script, $arg, $numKeys));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function evalRo(string $script, int $numKeys = 0, int|string ...$arg): mixed
    {
        return $this->run(static fn(Redis $r) => $r->eval_ro($script, $arg, $numKeys));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function evalSha(string $sha1, int $numKeys = 0, int|string ...$arg): mixed
    {
        return $this->run(static fn(Redis $r) => $r->evalSha($sha1, $arg, $numKeys));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function evalShaRo(string $sha1, int $numKeys = 0, int|string ...$arg): mixed
    {
        return $this->run(static fn(Redis $r) => $r->evalsha_ro($sha1, $arg, $numKeys));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function scriptExists(string ...$sha1): array
    {
        return array_map(boolval(...), $this->run(static fn(Redis $r) => $r->script('exists', ...$sha1)));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function scriptFlush(): void
    {
        $this->run(static fn(Redis $r) => $r->script('flush'));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function scriptLoad(string $script): string
    {
        return $this->run(static fn(Redis $r) => $r->script('load', $script));
    }

    # endregion SCRIPT ----------------------------------------------------------------------------------------------------

    # region SERVER ----------------------------------------------------------------------------------------------------

    /**
     * @inheritDoc
     */
    #[Override]
    public function acl(string $operation, string ...$args): mixed
    {
        return $this->run(static fn(Redis $r) => $r->acl($operation, ...$args));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function dbSize(): int
    {
        return $this->withCatch(function(): int {
            $nodes = iterator_to_array($this->getClientNodes());
            $sizes = array_map(static fn(Redis $n): int => $n->dbSize(), $nodes);
            return array_sum($sizes);
        });
    }

    # endregion SERVER ----------------------------------------------------------------------------------------------------

    # region STREAM ----------------------------------------------------------------------------------------------------

    /**
     * @inheritDoc
     */
    #[Override]
    public function xAdd(string $key, string $id, iterable $fields, ?int $maxLen = null, bool $approximate = false): string
    {
        $_fields = iterator_to_array($fields);
        $_maxLen = $maxLen ?? 0;
        return $this->run(static fn(Redis $r) => $r->xAdd($key, $id, $_fields, $_maxLen, $approximate));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function xDel(string $key, string ...$id): int
    {
        return $this->run(static fn(Redis $r) => $r->xDel($key, $id));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function xInfoStream(string $key, bool $full = false, ?int $count = null): array
    {
        return $this->run(static fn(Redis $r) => $r->xInfo('STREAM', $key, $full ? 'FULL' : null, $count ?? -1));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function xLen(string $key): int
    {
        return $this->run(static fn(Redis $r) => $r->xLen($key));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function xRange(string $key, string $start, string $end, ?int $count = null): array
    {
        return $this->run(static fn(Redis $r) => $r->xRange($key, $start, $end, $count ?? -1));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function xRead(iterable $streams, ?int $count = null, ?int $blockMilliseconds = null): array
    {
        $_streams = iterator_to_array($streams);
        $_count = $count ?? -1;
        $_blockMilliseconds = $blockMilliseconds ?? -1;
        return $this->run(static fn(Redis $r) => $r->xRead($_streams, $_count, $_blockMilliseconds));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function xRevRange(string $key, string $end, string $start, ?int $count = null): array
    {
        return $this->run(static fn(Redis $r) => $r->xRevRange($key, $end, $start, $count ?? -1));
    }

    /**
     * @inheritDoc
     */
    #[Override]
    public function xTrim(string $key, int|string $threshold, ?int $limit = null, XtrimMode $mode = XtrimMode::MaxLen, bool $approximate = false): int
    {
        $threshold = (string) $threshold;
        $minId = $mode === XtrimMode::MinId;
        return $limit === null
            ? $this->run(static fn(Redis $r) => $r->xTrim($key, $threshold, $approximate, $minId))
            : $this->run(static fn(Redis $r) => $r->xTrim($key, $threshold, $approximate, $minId, $limit));
    }

    # endregion STREAM -------------------------------------------------------------------------------------------------

    # region STRING ----------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/decr
     * @link https://redis.io/docs/commands/decrby
     * @param string $key
     * @param int $by
     * @return int
     * The decremented value
     */
    public function decr(string $key, int $by = 1): int
    {
        return $by === 1
            ? $this->run(static fn(Redis $r) => $r->decr($key))
            : $this->run(static fn(Redis $r) => $r->decrBy($key, $by));
    }

    /**
     * @link https://redis.io/docs/commands/decrbyfloat
     * @param string $key
     * @param float $by
     * @return float
     * The decremented value
     */
    public function decrByFloat(string $key, float $by): float
    {
        return $this->run(static fn(Redis $r) => $r->incrByFloat($key, -$by));
    }

    /**
     * @link https://redis.io/docs/commands/get
     * @param string $key
     * @return mixed|false
     * `false` if key does not exist.
     */
    public function get(string $key): mixed
    {
        return $this->run(static fn(Redis $r) => $r->get($key));
    }

    /**
     * @link https://redis.io/docs/commands/getdel
     * @param string $key
     * @return mixed|false
     * `false` if key does not exist.
     */
    public function getDel(string $key): mixed
    {
        return $this->run(static fn(Redis $r) => $r->getDel($key));
    }

    /**
     * @link https://redis.io/docs/commands/incr
     * @link https://redis.io/docs/commands/incrby
     * @param string $key
     * @param int $by
     * @return int
     * The incremented value
     */
    public function incr(string $key, int $by = 1): int
    {
        return $by === 1
            ? $this->run(static fn(Redis $r) => $r->incr($key))
            : $this->run(static fn(Redis $r) => $r->incrBy($key, $by));
    }

    /**
     * @link https://redis.io/docs/commands/incrbyfloat
     * @param string $key
     * @param float $by
     * @return float
     * The incremented value
     */
    public function incrByFloat(string $key, float $by): float
    {
        return $this->run(static fn(Redis $r) => $r->incrByFloat($key, $by));
    }

    /**
     * @link https://redis.io/docs/commands/mget
     * @param string ...$key
     * @return array<string, mixed|false>
     * Returns `[{retrieved_key} => value, ...]`. `false` if key is not found.
     */
    public function mGet(string ...$key): array
    {
        if (count($key) === 0) {
            return [];
        }
        $values = $this->run(static fn(Redis $r) => $r->mGet($key));
        $result = [];
        $index = 0;
        foreach ($key as $k) {
            $result[$k] = $values[$index];
            ++$index;
        }
        return $result;
    }

    /**
     * @link https://redis.io/docs/commands/mset
     * @param iterable<string, mixed> $pairs
     * @return void
     */
    public function mSet(iterable $pairs): void
    {
        $this->run(static fn(Redis $r) => $r->mSet(iterator_to_array($pairs)));
    }

    /**
     * @link https://redis.io/docs/commands/msetnx
     * @param iterable<string, mixed> $pairs
     * @return bool
     */
    public function mSetNx(iterable $pairs): bool
    {
        return $this->run(static fn(Redis $r) => $r->mSetNx(iterator_to_array($pairs)));
    }

    /**
     * @link https://redis.io/docs/commands/set
     * @param string $key
     * The key to set.
     * @param mixed $value
     * The value to set. Can be any type when serialization is enabled, can only be scalar type when disabled.
     * @param SetMode|null $mode
     * The mode to set the key. Can be `SetMode::Nx` or `SetMode::Xx`. Defaults to `null`.
     * @param int|null $ex
     * The number of seconds until the key will expire. Can not be used with `exAt`.
     * Defaults to `null`.
     * @param DateTimeInterface|null $exAt
     * The timestamp when the key will expire. Can not be used with `ex`.
     * Defaults to `null`.
     * @param bool $keepTtl
     * When set to `true`, the key will retain its ttl if key already exists.
     * Defaults to `false`.
     * @param bool $get
     * When set to `true`, the previous value of the key will be returned.
     * Defaults to `false`.
     * @return mixed
     */
    public function set(
        string $key,
        mixed $value,
        ?SetMode $mode = null,
        ?int $ex = null,
        ?DateTimeInterface $exAt = null,
        bool $keepTtl = false,
        bool $get = false,
    ): mixed
    {
        $options = [];
        if ($mode !== null) {
            $options[] = $mode->value;
        }
        if ($ex !== null) {
            $options['ex'] = $ex;
        }
        if ($exAt !== null) {
            $options['exat'] = $exAt->getTimestamp();
        }
        if ($keepTtl) {
            $options[] = 'keepttl';
        }
        if ($get) {
            $options[] = 'get';
        }

        return $this->run(static fn(Redis $r) => $r->set($key, $value, $options));
    }

    # endregion STRING -------------------------------------------------------------------------------------------------

}
