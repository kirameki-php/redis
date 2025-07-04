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
use Kirameki\Redis\Options\XTrimMode;
use function array_values;
use function count;
use function func_get_args as args;
use function hrtime;
use function iterator_to_array;

/**
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
 * - BLMPOP: waiting for PhpRedis to implement it
 * - TIME: doesn't work when using cluster of servers
 * - MOVE: not supported in cluster mode
 */
class RedisConnection
{
    /**
     * @template TConnectionConfig of ConnectionConfig
     * @param string $name,
     * @param Adapter<TConnectionConfig> $adapter
     * @param EventManager|null $events
     */
    public function __construct(
        public readonly string $name,
        public readonly Adapter $adapter,
        protected readonly ?EventManager $events = null,
    )
    {
    }

    /**
     * @return $this
     */
    public function connect(): static
    {
        $this->adapter->connect();
        $this->events?->emit(new ConnectionEstablished($this));
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
     * @param array<mixed> $args
     * @param Closure(Adapter<ConnectionConfig>, string, array<int, mixed>): mixed $callback
     * @return mixed
     */
    protected function run(string $command, array $args, Closure $callback): mixed
    {
        $then = hrtime(true);
        $result = $callback($this->adapter, $command, $args);
        $timeMs = (hrtime(true) - $then) * 1_000_000;
        $this->events?->emit(new CommandExecuted($this, $command, $args, $result, $timeMs));
        return $result;
    }

    # region CONNECTION ------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/client-id
     * @return int
     */
    public function clientId(): int
    {
        return $this->run('client', ['id'], static fn(Adapter $a) => $a->clientId());
    }

    /**
     * @link https://redis.io/docs/commands/client-info
     * @return array<string, ?scalar>
     */
    public function clientInfo(): array
    {
        return $this->run('client', ['info'], static fn(Adapter $a) => $a->clientInfo());
    }

    /**
     * @link https://redis.io/docs/commands/client-kill
     * @param string $ipAddressAndPort
     * @return bool
     */
    public function clientKill(string $ipAddressAndPort): bool
    {
        return $this->run(
            'client', ['kill', $ipAddressAndPort],
            static fn(Adapter $a) => $a->clientKill($ipAddressAndPort),
        );
    }

    /**
     * @link https://redis.io/docs/commands/client-list
     * @return list<array{ id: int, addr: string, laddr: string, fd: int, name: string, db: int }>
     */
    public function clientList(): array
    {
        return $this->run('client', ['list'], static fn(Adapter $a) => $a->clientList());
    }

    /**
     * @link https://redis.io/docs/commands/client-getname
     * @return string|null
     */
    public function clientGetname(): ?string
    {
        $result = $this->run('client', ['getname'], static fn(Adapter $a) => $a->clientGetname());
        return $result !== false ? $result : null;
    }

    /**
     * @link https://redis.io/docs/commands/client-setname
     * @param string $name
     * @return void
     */
    public function clientSetname(string $name): void
    {
        $this->run('client', ['setname'], static fn(Adapter $a) => $a->clientSetname($name));
    }

    /**
     * @link https://redis.io/docs/commands/echo
     * @param string $message
     * @return string
     */
    public function echo(string $message): string
    {
        return $this->run('echo', [$message], static fn(Adapter $a) => $a->echo($message));
    }

    /**
     * @link https://redis.io/docs/commands/ping
     * @return bool
     */
    public function ping(): bool
    {
        return $this->run('ping', [], static fn(Adapter $a) => $a->ping());
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
        return $this->run(__FUNCTION__, $key, static fn(Adapter $a) => $a->del(...$key));
    }

    /**
     * @link https://redis.io/docs/commands/exists
     * @param string ...$key
     * @return int
     */
    public function exists(string ...$key): int
    {
        return $this->run(__FUNCTION__, $key, static fn(Adapter $a) => $a->exists(...$key));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->expireTime($key));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->pExpireTime($key));
    }

    /**
     * @link https://redis.io/docs/commands/perist
     * @param string $key
     * @return bool
     */
    public function persist(string $key): bool
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->persist($key));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->expire($key, $seconds, $mode));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->pExpire($key, $milliseconds, $mode));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->expireAt($key, $time, $mode));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->pExpireAt($key, $time, $mode));
    }

    /**
     * @link https://redis.io/docs/commands/randomkey
     * @return string|null
     * Returns random key existing in server. Returns `null` if no key exists.
     */
    public function randomKey(): ?string
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->randomKey());
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->rename($srcKey, $dstKey));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->renameNx($srcKey, $dstKey));
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
        return $this->run(__FUNCTION__, $args, static function(Adapter $adapter) use ($pattern, $count, $prefixed) {
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->ttl($key));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->pTtl($key));
    }

    /**
     * @link https://redis.io/docs/commands/type
     * @param string $key
     * @return Type
     */
    public function type(string $key): Type
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->type($key));
    }

    /**
     * @link https://redis.io/docs/commands/unlink
     * @param string ...$key
     * @return int
     */
    public function unlink(string ...$key): int
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->unlink(...$key));
    }

    # endregion GENERIC ------------------------------------------------------------------------------------------------

    # region LIST ------------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/blpop
     * @param iterable<string> $keys
     * @param int|float $timeout  If no timeout is set, it will be set to 0 which is infinity.
     * @return array<string, mixed>|null  Returns null on timeout
     */
    public function blPop(iterable $keys, int|float $timeout = 0): ?array
    {
        $_keys = array_values(iterator_to_array($keys));
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->blPop($_keys, $timeout));
    }

    /**
     * @link https://redis.io/docs/commands/brpop
     * @param iterable<string> $keys
     * @param int|float $timeout  If no timeout is set, it will be set to 0 which is infinity.
     * @return array<string, mixed>|null  Returns null on timeout
     */
    public function brPop(iterable $keys, int|float $timeout = 0): ?array
    {
        $_keys = array_values(iterator_to_array($keys));
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->brPop($_keys, $timeout));
    }

    /**
     * @link https://redis.io/docs/commands/brpoplpush
     * @param string $source  Key of the source list.
     * @param string $destination  Key of the destination list.
     * @param int|float $timeout  If no timeout is set, it will be set to 0 which is infinity.
     * @return mixed|false  The value popped from the tail of the source list and pushed to the head of the destination list.
     * Returns `false` if the source list does not exist or if the operation timed out.
     */
    public function brPopLPush(string $source, string $destination, int|float $timeout = 0): mixed
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->brPopLPush($source, $destination, $timeout));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->lIndex($key, $index));
    }

    /**
     * @link https://redis.io/docs/commands/llen
     * @param string $key
     * @return int
     * The length of the list. 0 if the list does not exist.
     * Will throw CommandException if key set but is not a list.
     */
    public function lLen(string $key): int
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->lLen($key));
    }

    /**
     * @link https://redis.io/docs/commands/lpop
     * @param string $key
     * @return mixed|false  The value popped from the head of the list or `false` if the list does not exist.
     */
    public function lPop(string $key): mixed
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->lPop($key));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->lPush($key, ...$value));
    }

    /**
     * @link https://redis.io/docs/commands/lpushx
     * @param string $key
     * @param mixed $value
     * @return int
     * length of the list after the push operation.
     * Returns `0` if the key does not exist.
     */
    public function lPushx(string $key, mixed $value): int
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->lPushx($key, $value));
    }

    /**
     * @link https://redis.io/docs/commands/lrange
     * @param string $key
     * @param int $start  Can be negative to designate elements starting at the tail of the list.
     * @param int $end  Can be negative to designate elements starting at the tail of the list.
     * @return Vec<mixed>  List of elements in the specified range as Vec.
     */
    public function lRange(string $key, int $start, int $end): Vec
    {
        $result = $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->lRange($key, $start, $end));
        return new Vec($result);
    }

    /**
     * @link https://redis.io/docs/commands/lrem
     * @param string $key
     * @param mixed $value
     * @param int $count
     * The number of occurrences to remove.
     * If `0`, all occurrences will be removed.
     * If positive, it will remove from the head of the list to the tail.
     * If negative, it will remove from the tail of the list to the head.
     * @return mixed  The number of elements removed from the list.
     */
    public function lRem(string $key, mixed $value, int $count = 0): mixed
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->lRem($key, $value, $count));
    }

    /**
     * @link https://redis.io/docs/commands/lset
     * @param string $key
     * @param int $index  Zero based. Use negative indices to designate elements starting at the tail of the list.
     * @param mixed $value
     * @return bool
     * Returns `true` if the operation was successful.
     * Returns `false` if the index does not exist or is not a list.
     * @throws CommandException  if key does not exist.
     * @throws CommandException  if key set but is not a list.
     * @throws CommandException  if index is out of bounds.
     */
    public function lSet(string $key, int $index, mixed $value): bool
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->lSet($key, $index, $value));
    }

    /**
     * @link https://redis.io/docs/commands/ltrim
     * @param string $key
     * @param int $start  Can be negative to designate elements starting at the tail of the list.
     * @param int $end  Can be negative to designate elements starting at the tail of the list.
     * @return void
     */
    public function lTrim(string $key, int $start, int $end): void
    {
        $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->lTrim($key, $start, $end));
    }

    /**
     * @link https://redis.io/docs/commands/rpop
     * @param string $key
     * @return mixed|false  The value popped from the tail of the list or `false` if the list does not exist.
     */
    public function rPop(string $key): mixed
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->rPop($key));
    }

    /**
     * Moves the last element of the source list to the head of the destination list.
     * If the destination list does not exist, it will be created.
     *
     * @link https://redis.io/docs/commands/rpoplpush
     * @param string $srcKey  Key of the source list.
     * @param string $dstKey  Key of the destination list.
     * @return mixed|false
     * The value popped from the tail of the source list and pushed to the head of the destination list.
     * Returns `false` if the source list does not exist.
     */
    public function rPopLPush(string $srcKey, string $dstKey): mixed
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->rPopLPush($srcKey, $dstKey));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->rPush($key, ...$value));
    }

    /**
     * @link https://redis.io/docs/commands/rpushx
     * @param string $key
     * @param mixed $value
     * @return int
     * length of the list after the push operation.
     * Returns `0` if the key does not exist.
     */
    public function rPushx(string $key, mixed $value): int
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->rPushx($key, $value));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->eval($script, $numKeys, ...$arg));
    }

    /**
     * @link https://redis.io/commands/eval_ro
     * @param string $script
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    public function evalRo(string $script, int $numKeys = 0, int|string ...$arg): mixed
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->evalRo($script, $numKeys, ...$arg));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->evalSha($sha1, $numKeys, ...$arg));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->evalShaRo($sha1, $numKeys, ...$arg));
    }

    /**
     * @link https://redis.io/commands/script-exists
     * @param string ...$sha1
     * @return list<bool>
     */
    public function scriptExists(string ...$sha1): array
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->scriptExists(...$sha1));
    }

    /**
     * @link https://redis.io/commands/script-flush
     * @return void
     */
    public function scriptFlush(): void
    {
        $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->scriptFlush());
    }

    /**
     * @link https://redis.io/commands/script-load
     * @return string
     * The SHA1 digest of the script added into the script cache.
     */
    public function scriptLoad(string $script): string
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->scriptLoad($script));
    }

    # endregion SCRIPT -------------------------------------------------------------------------------------------------

    # region SERVER ----------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/acl
     * @param string $operation
     * @param string ...$args
     * @return mixed
     */
    public function acl(string $operation, string ...$args): mixed
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->acl($operation, ...$args));
    }

    /**
     * @link https://redis.io/docs/commands/dbsize
     * @return int
     */
    public function dbSize(): int
    {
        return $this->run(__FUNCTION__, [], static fn(Adapter $a) => $a->dbSize());
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
        $_fields = iterator_to_array($fields);
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xAdd($key, $id, $_fields, $maxLen, $approximate));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xDel($key, ...$id));
    }

    /**
     * @link https://redis.io/docs/commands/xinfo-stream
     * @param string $key
     * @param bool $full
     * @param int|null $count
     * limit the number of stream and PEL entries that are returned.
     * The default COUNT is 10 and COUNT of 0 means all entries will be returned.
     * @return array<string, mixed>
     */
    public function xInfoStream(string $key, bool $full = false, ?int $count = null): array
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xInfoStream($key, $full, $count));
    }

    /**
     * @link https://redis.io/docs/commands/xlen
     * @param string $key
     * @return int
     */
    public function xLen(string $key): int
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xLen($key));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xRange($key, $start, $end, $count));
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
        $_streams = iterator_to_array($streams);
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xRead($_streams, $count, $blockMilliseconds));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xRevRange($key, $end, $start, $count));
    }

    /**
     * @link https://redis.io/docs/commands/xtrim
     * @param string $key
     * @param int|string $threshold
     * @param int|null $limit
     * @param XTrimMode $mode
     * @param bool $approximate
     * @return int
     * The number of entries deleted.
     */
    public function xTrim(string $key, int|string $threshold, ?int $limit = null, XTrimMode $mode = XTrimMode::MaxLen, bool $approximate = false): int
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xTrim($key, $threshold, $limit, $mode, $approximate));
    }

    # endregion STREAM -------------------------------------------------------------------------------------------------

    # region STREAM GROUP-----------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/xack
     * @param string $key
     * @param string $group
     * @param iterable<int, string> $ids
     * @return int
     * The number of successfully acknowledged messages.
     */
    public function xAck(string $key, string $group, iterable $ids): int
    {
        $_ids = array_values(iterator_to_array($ids));
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xAck($key, $group, $_ids));
    }

    /**
     * @link https://redis.io/docs/commands/xclaim
     * @param string $key
     * @param string $group
     * @param string $consumer
     * @param int $minIdleTime
     * @param iterable<string> $ids
     * @return array<string, array<string, mixed>>
     */
    public function xClaim(string $key, string $group, string $consumer, int $minIdleTime, iterable $ids): array
    {
        $_ids = array_values(iterator_to_array($ids));
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xClaim($key, $group, $consumer, $minIdleTime, $_ids));
    }

    /**
     * @link https://redis.io/docs/commands/xgroup-create
     * @param string $key
     * @param string $group
     * @param string $id
     * @param bool $mkStream
     * @return void
     */
    public function xGroupCreate(string $key, string $group, string $id, bool $mkStream = false): void
    {
        $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xGroupCreate($key, $group, $id, $mkStream));
    }

    /**
     * @link https://redis.io/docs/commands/xgroup-createconsumer
     * @param string $key
     * @param string $group
     * @param string $consumer
     * @return int<0, 1>
     * The number of created consumers, either 0 or 1.
     */
    public function xGroupCreateConsumer(string $key, string $group, string $consumer): int
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xGroupCreateConsumer($key, $group, $consumer));
    }

    /**
     * @link https://redis.io/docs/commands/xgroup-delconsumer
     * @param string $key
     * @param string $group
     * @param string $consumer
     * @return int
     * The number of pending messages the consumer had before it was deleted.
     * If the consumer does not exist, 0 is returned.
     */
    public function xGroupDelConsumer(string $key, string $group, string $consumer): int
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xGroupDelConsumer($key, $group, $consumer));
    }

    /**
     * @link https://redis.io/docs/commands/xgroup-destroy
     * @param string $key
     * @param string $group
     * @return int<0, 1>
     * the number of destroyed consumer groups, either 0 or 1.
     */
    public function xGroupDestroy(string $key, string $group): int
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xGroupDestroy($key, $group));
    }

    /**
     * @link https://redis.io/docs/commands/xgroup-setid
     * @param string $key
     * @param string $group
     * @param string $id
     * @return void
     */
    public function xGroupSetId(string $key, string $group, string $id): void
    {
        $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xGroupSetId($key, $group, $id));
    }

    /**
     * @link https://redis.io/docs/commands/xinfo-consumers
     * @param string $key
     * @param string $group
     * @return list<array<string, scalar>>
     */
    public function xInfoConsumers(string $key, string $group): array
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xInfoConsumers($key, $group));
    }

    /**
     * @link https://redis.io/docs/commands/xinfo-groups
     * @param string $key
     * @return list<array<string, scalar>>
     */
    public function xInfoGroups(string $key): array
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xInfoGroups($key));
    }

    /**
     * @link https://redis.io/docs/commands/xreadgroup
     * @param string $group
     * @param string $consumer
     * @param iterable<string, string> $streams
     * @param int|null $count
     * @param int|null $blockMilliseconds
     * @return array<string, array<string, mixed>>
     */
    public function xReadGroup(string $group, string $consumer, iterable $streams, ?int $count = null, ?int $blockMilliseconds = null): array
    {
        $_streams = iterator_to_array($streams);
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xReadGroup($group, $consumer, $_streams, $count, $blockMilliseconds));
    }

    /**
     * @link https://redis.io/docs/commands/xpending
     * @param string $key
     * @param string $group
     * @param string $start
     * @param string $end
     * @param int $count
     * @return list<mixed>
     */
    public function xPending(string $key, string $group, string $start, string $end, int $count): array
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xPending($key, $group, $start, $end, $count));
    }

    /**
     * @link https://redis.io/docs/commands/xpending
     * @param string $key
     * @param string $group
     * @param string $consumer
     * @param string $start
     * @param string $end
     * @param int $count
     * @return list<mixed>
     */
    public function xPendingConsumer(string $key, string $group, string $consumer, string $start, string $end, int $count): array
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->xPending($key, $group, $start, $end, $count, $consumer));
    }

    # endregion STREAM GROUP -------------------------------------------------------------------------------------------

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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->decr($key, $by));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->decrByFloat($key, $by));
    }

    /**
     * @link https://redis.io/docs/commands/get
     * @param string $key
     * @return mixed|false
     * `false` if key does not exist.
     */
    public function get(string $key): mixed
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->get($key));
    }

    /**
     * @link https://redis.io/docs/commands/getdel
     * @param string $key
     * @return mixed|false
     * `false` if key does not exist.
     */
    public function getDel(string $key): mixed
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->getDel($key));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->incr($key, $by));
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
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->incrByFloat($key, $by));
    }

    /**
     * @link https://redis.io/docs/commands/mget
     * @param string ...$key
     * @return array<string, mixed|false>
     * Returns `[{retrieved_key} => value, ...]`. `false` if key is not found.
     */
    public function mGet(string ...$key): array
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->mGet(...$key));
    }

    /**
     * @link https://redis.io/docs/commands/mset
     * @param iterable<string, mixed> $pairs
     * @return void
     */
    public function mSet(iterable $pairs): void
    {
        $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->mSet(iterator_to_array($pairs)));
    }

    /**
     * @link https://redis.io/docs/commands/msetnx
     * @param iterable<string, mixed> $pairs
     * @return bool
     */
    public function mSetNx(iterable $pairs): bool
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->mSetNx(iterator_to_array($pairs)));
    }

    /**
     * TODO add PX test
     * @link https://redis.io/docs/commands/set
     * @param string $key
     * The key to set.
     * @param mixed $value
     * The value to set. Can be any type when serialization is enabled, can only be scalar type when disabled.
     * @param SetMode|null $mode
     * The mode to set the key. Can be `SetMode::Nx` or `SetMode::Xx`. Defaults to `null`.
     * @param int|null $ex
     *  The number of seconds until the key will expire. Can not be used with `px`, `exAt`.
     *  Defaults to `null`.
     * @param DateTimeInterface|null $exAt
     *  * The timestamp when the key will expire. Can not be used with `ex`, `px`, `pxAt`.
     * @param int|null $px
     *  *  The number of milliseconds until the key will expire. Can not be used with `ex`, `exAt`, or `pxAt`.
     *  * Defaults to `null`.
     * @param DateTimeInterface|null $pxAt
     *  * The timestamp when the key will expire. Can not be used with `ex`, `px`, `exAt`.
     *  Defaults to `null`.
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
        ?int $px = null,
        ?DateTimeInterface $pxAt = null,
        bool $keepTtl = false,
        bool $get = false,
    ): mixed
    {
        return $this->run(__FUNCTION__, args(), static fn(Adapter $a) => $a->set($key, $value, $mode, $ex, $exAt, $px, $pxAt, $keepTtl, $get));
    }

    # endregion STRING -------------------------------------------------------------------------------------------------
}
