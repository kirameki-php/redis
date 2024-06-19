<?php declare(strict_types=1);

namespace Kirameki\Redis;

use Closure;
use DateTimeInterface;
use Kirameki\Collections\LazyIterator;
use Kirameki\Collections\Vec;
use Kirameki\Event\EventManager;
use Kirameki\Redis\Adapters\Adapter;
use Kirameki\Redis\Config\ConnectionConfig;
use Kirameki\Redis\Events\CommandExecuted;
use Kirameki\Redis\Events\ConnectionEstablished;
use Kirameki\Redis\Exceptions\CommandException;
use Kirameki\Redis\Options\SetMode;
use Kirameki\Redis\Options\TtlMode;
use Kirameki\Redis\Options\Type;
use LogicException;
use Redis;
use function array_map;
use function count;
use function dump;
use function func_get_args;
use function hrtime;
use function iterator_to_array;
use function strtolower;

/**
 * HASHES --------------------------------------------------------------------------------------------------------------
 * @method mixed hDel(string $key, string $field)
 * @method bool hExists(string $key, string $field)
 * @method mixed hGet(string $key, string $field)
 * @method array hGetAll(string $key)
 * @method mixed hIncrBy(string $key, string $field, int $amount)
 * @method array hKeys(string $key)
 * @method int hLen(string $key)
 * @method mixed hSet(string $key, string $field, $value)
 * @method mixed hSetNx(string $key, string $field, $value)
 * @method array hVals(string $key)
 *
 * LISTS ---------------------------------------------------------------------------------------------------------------
 * @method mixed  brPop(string[] $key, int $timeout)
 * @method mixed  brpoplpush(string $source, string $destination, int $timeout)
 * @method mixed  lLen($key)
 * @method mixed  lPop(string $key)
 * @method mixed  lPushx(string $key, $value)
 * @method mixed  lRange(string $key, int $start, int $end)
 * @method mixed  lRem(string $key, $value, int $count)
 * @method mixed  lSet(string $key, int $index, $value)
 * @method mixed  lTrim(string $key, int $start, int $end)
 * @method mixed  rPop(string $key)
 * @method mixed  rpoplpush(string $source, string $destination)
 * @method mixed  rPushx(string $key, $value)
 *
 * SORTED SETS ---------------------------------------------------------------------------------------------------------
 * @method array bzPopMax(string|array $key, int $timeout) // A timeout of zero can be used to block indefinitely
 * @method array bzPopMin(string|array $key, int $timeout) // A timeout of zero can be used to block indefinitely
 * @method int zAdd(string $key, array $options, float $score, string $member, ...$scoreThenMember) // options: ['NX', 'XX', 'CH', 'INCR']
 * @method int zCard(string $key)
 * @method int zCount(string $key, string $start, string $end)
 * @method int zIncrBy(string $key, float $increment, string $member)
 * @method int zInterStore(string $output, $zSetKeys, array $weight = null, string $aggregateFunction = 'SUM')
 * @method int zLexCount(string $key, int $min, int $max)
 * @method array zPopMax(string $key, int $count = 1)
 * @method array zPopMin(string $key, int $count = 1)
 * @method array zRange(string $key, int $start, int $end, bool|null $withScores = null)
 * @method array|bool zRangeByLex(string $key, int $min, int $max, int $offset = null, int $limit = null)
 * @method array|bool zRangeByScore(string $key, int $start, int $end, array $options = [])  // options: { withscores => bool, limit => [$offset, $count] }
 * @method bool|int zRank(string $key, string $member)
 * @method bool|int zRem(string $key, string ...$members)
 * @method bool|int zRemRangeByRank(string $key, int $start, int $end)
 * @method bool|int zRemRangeByScore(string $key, float|string $start, float|string $end)
 * @method array zRevRange(string $key, int $start, int $end, bool|null $withScores = null)
 * @method array zRevRangeByLex(string $key, int $min, int $max, int $offset = null, int $limit = null)
 * @method array|bool zRevRangeByScore(string $key, int $start, int $end, array $options = [])  // options: { withscores => bool, limit => [$offset, $count] }
 * @method bool|int zRevRank(string $key, string $member)
 * @method bool|int zScore(string $key, string $member)
 * @method bool|int zUnionStore(string $output, array $zSetKeys, array $weights = null, string $aggregateFunction = 'SUM')
 *
 * STRING --------------------------------------------------------------------------------------------------------------
 * UNSUPPORTED COMMANDS
 * - APPEND: does not work well with serialization
 * - BLPOP: waiting for PhpRedis to implement it
 * - BLMPOP: waiting for PhpRedis to implement it
 * - TIME: doesn't work when using cluster of servers
 * - MOVE: not supported in cluster mode
 */
class Connection
{
    /**
     * @template TConnectionConfig of ConnectionConfig
     * @param string $name,
     * @param Adapter<TConnectionConfig> $adapter
     * @param EventManager $events
     */
    public function __construct(
        public readonly string $name,
        public readonly Adapter $adapter,
        protected readonly EventManager $events,
    )
    {
    }

    /**
     * @return $this
     */
    public function connect(): static
    {
        $this->adapter->connect();
        $this->events->emit(new ConnectionEstablished($this));
        return $this;
    }

    /**
     * @return bool
     */
    public function disconnect(): bool
    {
        return $this->adapter->disconnect();
    }

    /**
     * @return $this
     */
    public function reconnect(): static
    {
        $this->disconnect();
        return $this->connect();
    }

    /**
     * @return bool
     */
    public function isConnected(): bool
    {
        return $this->adapter->isConnected();
    }

    /**
     * @param string $command
     * @param mixed ...$args
     * @return mixed
     */
    public function run(string $command, mixed ...$args): mixed
    {
        return $this->process($command, $args, static function(Adapter $adapter, string $name, array $args) {
            return $adapter->command($name, $args);
        });
    }

    /**
     * @param string $command
     * @param mixed ...$args
     * @return mixed
     */
    public function runRaw(string $command, mixed ...$args): mixed
    {
        return $this->process($command, $args, static function(Adapter $adapter, string $name, array $args) {
            return $adapter->rawCommand($name, $args);
        });
    }

    /**
     * @param string $command
     * @param array<mixed> $args
     * @param Closure(Adapter<ConnectionConfig>, string, array<int, mixed>): mixed $callback
     * @return mixed
     */
    protected function process(string $command, array $args, Closure $callback): mixed
    {
        $then = hrtime(true);
        $result = $callback($this->adapter, $command, $args);
        $timeMs = (hrtime(true) - $then) * 1_000_000;
        $this->events->emit(new CommandExecuted($this, $command, $args, $result, $timeMs));
        return $result;
    }

    # region CONNECTION ------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/client-id
     * @return int
     */
    public function clientId(): int
    {
        return $this->run('client', 'id');
    }

    /**
     * @link https://redis.io/docs/commands/client-info
     * @return array<string, ?scalar>
     */
    public function clientInfo(): array
    {
        return $this->run('client', 'info');
    }

    /**
     * @link https://redis.io/docs/commands/client-kill
     * @param string $ipAddressAndPort
     * @return bool
     */
    public function clientKill(string $ipAddressAndPort): bool
    {
        return $this->run('client', 'kill', $ipAddressAndPort);
    }

    /**
     * @link https://redis.io/docs/commands/client-list
     * @return list<array{ id: int, addr: string, laddr: string, fd: int, name: string, db: int }>
     */
    public function clientList(): array
    {
        return $this->run('client', 'list');
    }

    /**
     * @link https://redis.io/docs/commands/client-getname
     * @return string|null
     */
    public function clientGetname(): ?string
    {
        $result = $this->run('client', 'getname');
        return $result !== false ? $result : null;
    }

    /**
     * @link https://redis.io/docs/commands/client-setname
     * @param string $name
     * @return void
     */
    public function clientSetname(string $name): void
    {
        $this->run('client', 'setname', $name);
    }

    /**
     * @link https://redis.io/docs/commands/echo
     * @param string $message
     * @return string
     */
    public function echo(string $message): string
    {
        return $this->run(__FUNCTION__, $message);
    }

    /**
     * @link https://redis.io/docs/commands/ping
     * @return bool
     */
    public function ping(): bool
    {
        return $this->run(__FUNCTION__);
    }

    # endregion CONNECTION ---------------------------------------------------------------------------------------------

    # region GENERIC ---------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/del
     * @param string ...$key
     * @return int
     * Returns the number of keys that were removed.
     */
    public function del(string ...$key): int
    {
        if (count($key) === 0) {
            return 0;
        }
        return $this->run(__FUNCTION__, ...$key);
    }

    /**
     * @link https://redis.io/docs/commands/exists
     * @param string ...$key
     * @return int
     */
    public function exists(string ...$key): int
    {
        if (count($key) === 0) {
            return 0;
        }
        return $this->run(__FUNCTION__, ...$key);
    }

    /**
     * @link https://redis.io/docs/commands/expiretime
     * @param string $key
     * @return int|false|null
     * Returns the remaining time to live of a key that has a timeout.
     * Returns `null` if key exists but has no associated expire.
     * Returns `false` if key does not exist.
     */
    public function expireTime(string $key): int|false|null
    {
        $result = $this->run(strtolower(__FUNCTION__), $key);
        return match($result) {
            -2 => false,
            -1 => null,
            default => $result,
        };
    }

    /**
     * @link https://redis.io/docs/commands/pexpiretime
     * @param string $key
     * @return int|false|null
     * Returns the remaining time to live of a key that has a timeout.
     * Returns `null` if key exists but has no associated expire.
     * Returns `false` if key does not exist.
     */
    public function pExpireTime(string $key): int|false|null
    {
        $result = $this->run(strtolower(__FUNCTION__), $key);
        return match($result) {
            -2 => false,
            -1 => null,
            default => $result,
        };
    }

    /**
     * @link https://redis.io/docs/commands/perist
     * @param string $key
     * @return bool
     */
    public function persist(string $key): bool
    {
        return $this->run(__FUNCTION__, $key);
    }

    /**
     * @link https://redis.io/docs/commands/expire
     * @param string $key
     * @param int $seconds
     * @param TtlMode|null $mode
     * @return bool
     */
    public function expire(string $key, int $seconds, ?TtlMode $mode = null): bool
    {
        return $this->run(__FUNCTION__, $key, $seconds, $mode?->value);
    }

    /**
     * @link https://redis.io/docs/commands/pexpire
     * @param string $key
     * @param int $milliseconds
     * @param TtlMode|null $mode
     * @return bool
     */
    public function pExpire(string $key, int $milliseconds, ?TtlMode $mode = null): bool
    {
        return $this->run('pexpire', $key, $milliseconds, $mode?->value);
    }

    /**
     * @link https://redis.io/docs/commands/expireat
     * @param string $key
     * @param DateTimeInterface $time
     * @param TtlMode|null $mode
     * @return bool
     */
    public function expireAt(string $key, DateTimeInterface $time, ?TtlMode $mode = null): bool
    {
        return $this->run(__FUNCTION__, $key, $time->getTimestamp(), $mode?->value);
    }

    /**
     * @link https://redis.io/docs/commands/pexpireat
     * @param string $key
     * @param DateTimeInterface $time
     * @param TtlMode|null $mode
     * @return bool
     */
    public function pExpireAt(string $key, DateTimeInterface $time, ?TtlMode $mode = null): bool
    {
        return $this->run('pexpireAt', $key, $time->getTimestamp() * 1000, $mode?->value);
    }

    /**
     * @link https://redis.io/docs/commands/randomkey
     * @return string|null
     * Returns random key existing in server. Returns `null` if no key exists.
     */
    public function randomKey(): ?string
    {
        $result = $this->run(__FUNCTION__);
        return $result !== false ? $result : null;
    }

    /**
     * @link https://redis.io/docs/commands/rename
     * @param string $srcKey
     * @param string $dstKey
     * @return bool
     * `true` in case of success, `false` in case of failure
     * @throws CommandException
     * "ERR no such key" is thrown if no key exists.
     */
    public function rename(string $srcKey, string $dstKey): bool
    {
        return $this->run(__FUNCTION__, $srcKey, $dstKey);
    }

    /**
     * @link https://redis.io/docs/commands/renamenx
     * @param string $srcKey
     * @param string $dstKey
     * @return bool
     * `true` in case of success, `false` in case of failure
     * @throws CommandException
     * "ERR no such key" is thrown if no key exists.
     */
    public function renameNx(string $srcKey, string $dstKey): bool
    {
        return $this->run(__FUNCTION__, $srcKey, $dstKey);
    }

    /**
     *
     * Will iterate through the set of keys that match `$pattern` or all keys if no pattern is given.
     * Scan has the following limitations
     * - A given element may be returned multiple times.
     *
     * @link https://redis.io/docs/commands/scan
     * @param string $pattern
     * Patterns to be scanned. (default: `*`)
     * @param int $count
     * Number of elements returned per iteration. This is just a hint and is not guaranteed. (default: `10_000`)
     * @param bool $prefixed
     * If set to `true`, result will contain the prefix set in the config. (default: `false`)
     * @return Vec<string>
     */
    public function scan(string $pattern = '*', int $count = 10_000, bool $prefixed = false): Vec
    {
        $args = func_get_args();
        return $this->process(__FUNCTION__, $args, static function(Adapter $adapter) use ($pattern, $count, $prefixed) {
            $generator = $adapter->scan($pattern, $count, $prefixed);
            return new Vec(new LazyIterator($generator));
        });
    }

    /**
     * @link https://redis.io/docs/commands/ttl
     * @param string $key
     * @return int|false|null
     * Returns the remaining time to live of a key that has a timeout.
     * Returns `null` if key exists but has no associated expire.
     * Returns `false` if key does not exist.
     */
    public function ttl(string $key): int|false|null
    {
        $result = $this->run(__FUNCTION__, $key);
        return match($result) {
            -2 => false,
            -1 => null,
            default => $result,
        };
    }

    /**
     * @link https://redis.io/docs/commands/pttl
     * @param string $key
     * @return int|false|null
     * Returns the remaining time to live of a key that has a timeout.
     * Returns `null` if key exists but has no associated expire.
     * Returns `false` if key does not exist.
     */
    public function pTtl(string $key): int|false|null
    {
        $result = $this->run(__FUNCTION__, $key);
        return match($result) {
            -2 => false,
            -1 => null,
            default => $result,
        };
    }

    /**
     * @link https://redis.io/docs/commands/type
     * @param string $key
     * @return Type
     */
    public function type(string $key): Type
    {
        $type = $this->run(__FUNCTION__, $key);
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
     * @link https://redis.io/docs/commands/unlink
     * @param string ...$key
     * @return int
     */
    public function unlink(string ...$key): int
    {
        return $this->run(__FUNCTION__, ...$key);
    }

    # endregion GENERIC ------------------------------------------------------------------------------------------------

    # region SERVER ----------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/acl
     * @param string $operation
     * @param string ...$args
     * @return mixed
     */
    public function acl(string $operation, string ...$args): mixed
    {
        return $this->run(__FUNCTION__, $operation, ...$args);
    }

    /**
     * @link https://redis.io/docs/commands/dbsize
     * @return int
     */
    public function dbSize(): int
    {
        return $this->process(__FUNCTION__, [], static fn(Adapter $a) => $a->dbSize());
    }

    /**
     * @param int $per
     * Suggest number of keys to get per scan.
     * @return int
     * Returns the number of keys deleted.
     */
    public function flushKeys(int $per = 100_000): int
    {
        $count = 0;
        do {
            $keys = $this->scan('*', $per)->toArray();
            $count += $this->del(...$keys);
        } while (count($keys) !== 0);
        return $count;
    }

    # endregion SERVER -------------------------------------------------------------------------------------------------

    # region STREAM ----------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/xadd
     * @param string $key
     * @param string $id
     * @param iterable<string, mixed> $fields
     * @param int|null $maxLen
     * @param bool $approximate
     * @return string
     */
    public function xAdd(string $key, string $id, iterable $fields, ?int $maxLen = null, bool $approximate = false): string
    {
        return $this->run(__FUNCTION__, $key, $id, iterator_to_array($fields), $maxLen ?? 0, $approximate);
    }

    /**
     * @link https://redis.io/docs/commands/xdel
     * @param string $key
     * @param string ...$id
     * @return int
     * The number of entries deleted.
     */
    public function xDel(string $key, string ...$id): int
    {
        return $this->run(__FUNCTION__, $key, $id);
    }

    /**
     * @link https://redis.io/docs/commands/xlen
     * @param string $key
     * @return int
     */
    public function xLen(string $key): int
    {
        return $this->run(__FUNCTION__, $key);
    }

    /**
     * @link https://redis.io/docs/commands/xrange
     * @param string $key
     * @param string $start
     * @param string $end
     * @param int|null $count
     * Set to a positive integer to limit the number of entries returned.
     * Set to `null` to return all entries.
     * @return array<string, array<string, mixed>>
     * A list of stream entries with IDs matching the specified range.
     */
    public function xRange(string $key, string $start, string $end, ?int $count = null): array
    {
        return $this->run(__FUNCTION__, $key, $start, $end, $count ?? -1);
    }

    /**
     * [!NOTE] When `blockMilliseconds` > 0, the method will return `null` on timeout.
     * [!WARNING] The process will not respond to any signals until the block timeout is reached.
     *
     * @link https://redis.io/docs/commands/xread
     * @param iterable<string, string> $streams
     * @param int|null $count
     * Set to a positive integer to limit the number of entries returned.
     * Set to `null` to return all entries.
     * @param int|null $blockMilliseconds
     * Set to a positive integer to block for that many milliseconds.
     * Set to `0` to block indefinitely.
     * Set to `null` to return immediately.
     * @return array<string, array<string, mixed>>
     * Returns an array of streams and their entries.
     * Format: { stream => { id => { field => value, ... }, ... }
     */
    public function xRead(iterable $streams, ?int $count = null, ?int $blockMilliseconds = null): array
    {
        return $this->run(__FUNCTION__, iterator_to_array($streams), $count ?? -1, $blockMilliseconds ?? -1);
    }

    /**
     * @link https://redis.io/docs/commands/xrevrange
     * @param string $key
     * @param string $end
     * @param string $start
     * @param int|null $count
     * @return array<string, array<string, mixed>>
     * A list of stream entries with IDs matching the specified range.
     */
    public function xRevRange(string $key, string $end, string $start, ?int $count = null): array
    {
        return $this->run(__FUNCTION__, $key, $end, $start, $count ?? -1);
    }

    /**
     * @link https://redis.io/docs/commands/xtrim
     * @param string $key
     * @param int $maxLen
     * @param bool $approximate
     * @return int
     * The number of entries deleted.
     */
    public function xTrim(string $key, int $maxLen, bool $approximate = false): int
    {
        return $this->run(__FUNCTION__, $key, $maxLen, $approximate);
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
            ? $this->run(__FUNCTION__, $key)
            : $this->run(__FUNCTION__ . 'By', $key, $by);
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
        return $this->run('incrByFloat', $key, -$by);
    }

    /**
     * @link https://redis.io/docs/commands/get
     * @param string $key
     * @return mixed|false
     * `false` if key does not exist.
     */
    public function get(string $key): mixed
    {
        return $this->run(__FUNCTION__, $key);
    }

    /**
     * @link https://redis.io/docs/commands/getdel
     * @param string $key
     * @return mixed|false
     * `false` if key does not exist.
     */
    public function getDel(string $key): mixed
    {
        return $this->run(__FUNCTION__, $key);
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
            ? $this->run(__FUNCTION__, $key)
            : $this->run(__FUNCTION__ . 'By', $key, $by);
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
        return $this->run(__FUNCTION__, $key, $by);
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
        $values = $this->run(__FUNCTION__, $key);
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
        $this->run(__FUNCTION__, $pairs);
    }

    /**
     * @link https://redis.io/docs/commands/msetnx
     * @param iterable<string, mixed> $pairs
     * @return bool
     */
    public function mSetNx(iterable $pairs): bool
    {
        return $this->run(__FUNCTION__, $pairs);
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

        return $this->run(__FUNCTION__, $key, $value, $options);
    }

    # endregion STRING -------------------------------------------------------------------------------------------------

    # region LIST ------------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/blpop
     * @param iterable<string> $keys
     * @param int|float $timeout  If no timeout is set, it will be set to 0 which is infinity.
     * @return array<string, mixed>|null  Returns null on timeout
     */
    public function blPop(iterable $keys, int|float $timeout = 0): ?array
    {
        $keys = iterator_to_array($keys);

        /** @var array{ 0?: string, 1?: mixed } $result */
        $result = $this->run(__FUNCTION__, $keys, $timeout);

        return (count($result) > 0)
            ? [$result[0] => $result[1]]
            : null;
    }

    /**
     * @link https://redis.io/docs/commands/lindex
     * @param string $key
     * @param int $index  Zero based. Use negative indices to designate elements starting at the tail of the list.
     * @return mixed|false  The value at index or `false` if... (1) key is missing or (2) index is missing.
     * @throws CommandException  if key set but is not a list.
     */
    public function lIndex(string $key, int $index): mixed
    {
        return $this->run(__FUNCTION__, $key, $index);
    }

    /**
     * Each element is inserted to the head of the list, from the leftmost to the rightmost element.
     * Ex: `$client->lPush('mylist', 'a', 'b', 'c')` will create a list `["c", "b", "a"]`
     *
     * @link https://redis.io/docs/commands/lpush
     * @param string $key
     * @param mixed ...$value
     * @return int  length of the list after the push operation.
     */
    public function lPush(string $key, mixed ...$value): int
    {
        return $this->run(__FUNCTION__, $key, ...$value);
    }

    /**
     * Each element is inserted to the tail of the list, from the leftmost to the rightmost element.
     * Ex: `$client->rPush('mylist', 'a', 'b', 'c')` will create a list `["a", "b", "c"]`.
     *
     * @link https://redis.io/docs/commands/rpush
     * @param string $key
     * @param mixed ...$value
     * @return int  length of the list after the push operation.
     */
    public function rPush(string $key, mixed ...$value): int
    {
        return $this->run(__FUNCTION__, $key, ...$value);
    }

    # endregion LIST ---------------------------------------------------------------------------------------------------

    # region SCRIPT ----------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/commands/eval
     * @param string $script
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    public function eval(string $script, int $numKeys = 0, int|string ...$arg): mixed
    {
        return $this->run(__FUNCTION__, $script, $arg, $numKeys);
    }

    /**
     * @link https://redis.io/commands/evalsha_ro
     * @param string $script
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    public function evalRo(string $script, int $numKeys = 0, int|string ...$arg): mixed
    {
        return $this->runRaw('eval_ro', $script, $numKeys, ...$arg);
    }

    /**
     * @link https://redis.io/commands/evalsha
     * @param string $sha1
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    public function evalSha(string $sha1, int $numKeys = 0, int|string ...$arg): mixed
    {
        return $this->run(__FUNCTION__, $sha1, $arg, $numKeys);
    }

    /**
     * @link https://redis.io/commands/evalsha_ro
     * @param string $sha1
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    public function evalShaRo(string $sha1, int $numKeys = 0, int|string ...$arg): mixed
    {
        return $this->runRaw('evalsha_ro', $sha1, $numKeys, ...$arg);
    }

    /**
     * @link https://redis.io/commands/script-exists
     * @param string ...$sha1
     * @return list<bool>
     */
    public function scriptExists(string ...$sha1): array
    {
        return array_map(boolval(...), $this->run('script', 'exists', ...$sha1));
    }

    /**
     * @link https://redis.io/commands/script-flush
     * @return void
     */
    public function scriptFlush(): void
    {
        $this->run('script', 'flush');
    }

    /**
     * @link https://redis.io/commands/script-load
     * @return string
     * The SHA1 digest of the script added into the script cache.
     */
    public function scriptLoad(string $script): string
    {
        return $this->run('script', 'load', $script);
    }

    # endregion SCRIPT -------------------------------------------------------------------------------------------------
}
