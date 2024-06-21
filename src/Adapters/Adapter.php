<?php declare(strict_types=1);

namespace Kirameki\Redis\Adapters;

use DateTimeInterface;
use Generator;
use Kirameki\Redis\Config\ConnectionConfig;
use Kirameki\Redis\Exceptions\CommandException;
use Kirameki\Redis\Options\SetMode;
use Kirameki\Redis\Options\TtlMode;
use Kirameki\Redis\Options\Type;
use Kirameki\Redis\Options\XtrimMode;

/**
 * @template TConnectionConfig of ConnectionConfig
 */
abstract class Adapter
{
    /**
     * @param TConnectionConfig $config
     */
    public function __construct(
        public readonly ConnectionConfig $config,
    )
    {
    }

    /**
     * @return $this
     */
    abstract public function connect(): static;

    /**
     * @return bool
     */
    abstract public function disconnect(): bool;

    /**
     * @return bool
     */
    abstract public function isConnected(): bool;

    /**
     * @param string $name
     * @param array<mixed> $args
     * @return mixed
     */
    abstract public function rawCommand(string $name, array $args): mixed;

    # region CONNECTION ------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/client-id
     * @return int
     */
    abstract public function clientId(): int;

    /**
     * @link https://redis.io/docs/commands/client-info
     * @return array<string, ?scalar>
     */
    abstract public function clientInfo(): array;

    /**
     * @link https://redis.io/docs/commands/client-kill
     * @param string $ipAddressAndPort
     * @return bool
     */
    abstract public function clientKill(string $ipAddressAndPort): bool;

    /**
     * @link https://redis.io/docs/commands/client-list
     * @return list<array{ id: int, addr: string, laddr: string, fd: int, name: string, db: int }>
     */
    abstract public function clientList(): array;

    /**
     * @link https://redis.io/docs/commands/client-getname
     * @return string|null
     */
    abstract public function clientGetname(): ?string;

    /**
     * @link https://redis.io/docs/commands/client-setname
     * @param string $name
     * @return void
     */
    abstract public function clientSetname(string $name): void;

    /**
     * @link https://redis.io/docs/commands/echo
     * @param string $message
     * @return string
     */
    abstract public function echo(string $message): string;

    /**
     * @link https://redis.io/docs/commands/ping
     * @return bool
     */
    abstract public function ping(): bool;

    # endregion CONNECTION ---------------------------------------------------------------------------------------------

    # region GENERIC ---------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/del
     * @param string ...$key
     * @return int
     * Returns the number of keys that were removed.
     */
    abstract public function del(string ...$key): int;

    /**
     * @link https://redis.io/docs/commands/exists
     * @param string ...$key
     * @return int
     */
    abstract public function exists(string ...$key): int;

    /**
     * @link https://redis.io/docs/commands/expiretime
     * @param string $key
     * @return int|false|null
     * Returns the remaining time to live of a key that has a timeout.
     * Returns `null` if key exists but has no associated expire.
     * Returns `false` if key does not exist.
     */
    abstract public function expireTime(string $key): int|false|null;

    /**
     * @link https://redis.io/docs/commands/pexpiretime
     * @param string $key
     * @return int|false|null
     * Returns the remaining time to live of a key that has a timeout.
     * Returns `null` if key exists but has no associated expire.
     * Returns `false` if key does not exist.
     */
    abstract public function pExpireTime(string $key): int|false|null;

    /**
     * @link https://redis.io/docs/commands/perist
     * @param string $key
     * @return bool
     */
    abstract public function persist(string $key): bool;

    /**
     * @link https://redis.io/docs/commands/expire
     * @param string $key
     * @param int $seconds
     * @param TtlMode|null $mode
     * @return bool
     */
    abstract public function expire(string $key, int $seconds, ?TtlMode $mode = null): bool;

    /**
     * @link https://redis.io/docs/commands/pexpire
     * @param string $key
     * @param int $milliseconds
     * @param TtlMode|null $mode
     * @return bool
     */
    abstract public function pExpire(string $key, int $milliseconds, ?TtlMode $mode = null): bool;

    /**
     * @link https://redis.io/docs/commands/expireat
     * @param string $key
     * @param DateTimeInterface $time
     * @param TtlMode|null $mode
     * @return bool
     */
    abstract public function expireAt(string $key, DateTimeInterface $time, ?TtlMode $mode = null): bool;

    /**
     * @link https://redis.io/docs/commands/pexpireat
     * @param string $key
     * @param DateTimeInterface $time
     * @param TtlMode|null $mode
     * @return bool
     */
    abstract public function pExpireAt(string $key, DateTimeInterface $time, ?TtlMode $mode = null): bool;

    /**
     * @link https://redis.io/docs/commands/randomkey
     * @return string|null
     * Returns random key existing in server. Returns `null` if no key exists.
     */
    abstract public function randomKey(): ?string;

    /**
     * @link https://redis.io/docs/commands/rename
     * @param string $srcKey
     * @param string $dstKey
     * @return bool
     * `true` in case of success, `false` in case of failure
     * @throws CommandException
     * "ERR no such key" is thrown if no key exists.
     */
    abstract public function rename(string $srcKey, string $dstKey): bool;

    /**
     * @link https://redis.io/docs/commands/renamenx
     * @param string $srcKey
     * @param string $dstKey
     * @return bool
     * `true` in case of success, `false` in case of failure
     * @throws CommandException
     * "ERR no such key" is thrown if no key exists.
     */
    abstract public function renameNx(string $srcKey, string $dstKey): bool;

    /**
     * @link https://redis.io/docs/commands/scan
     * @param string $pattern
     * @param int $count
     * @param bool $prefixed
     * @return Generator<int, string>
     */
    abstract public function scan(string $pattern = '*', int $count = 10_000, bool $prefixed = false): Generator;

    /**
     * @link https://redis.io/docs/commands/ttl
     * @param string $key
     * @return int|false|null
     * Returns the remaining time to live of a key that has a timeout.
     * Returns `null` if key exists but has no associated expire.
     * Returns `false` if key does not exist.
     */
    abstract public function ttl(string $key): int|false|null;

    /**
     * @link https://redis.io/docs/commands/pttl
     * @param string $key
     * @return int|false|null
     * Returns the remaining time to live of a key that has a timeout.
     * Returns `null` if key exists but has no associated expire.
     * Returns `false` if key does not exist.
     */
    abstract public function pTtl(string $key): int|false|null;

    /**
     * @link https://redis.io/docs/commands/type
     * @param string $key
     * @return Type
     */
    abstract public function type(string $key): Type;

    /**
     * @link https://redis.io/docs/commands/unlink
     * @param string ...$key
     * @return int
     */
    abstract public function unlink(string ...$key): int;

    # endregion GENERIC ------------------------------------------------------------------------------------------------

    # region LIST ------------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/blpop
     * @param iterable<string> $keys
     * @param int|float $timeout  If no timeout is set, it will be set to 0 which is infinity.
     * @return array<string, mixed>|null  Returns null on timeout
     */
    abstract public function blPop(iterable $keys, int|float $timeout = 0): ?array;

    /**
     * @link https://redis.io/docs/commands/lindex
     * @param string $key
     * @param int $index  Zero based. Use negative indices to designate elements starting at the tail of the list.
     * @return mixed|false  The value at index or `false` if... (1) key is missing or (2) index is missing.
     * @throws CommandException  if key set but is not a list.
     */
    abstract public function lIndex(string $key, int $index): mixed;

    /**
     * Each element is inserted to the head of the list, from the leftmost to the rightmost element.
     * Ex: `$client->lPush('mylist', 'a', 'b', 'c')` will create a list `["c", "b", "a"]`
     *
     * @link https://redis.io/docs/commands/lpush
     * @param string $key
     * @param mixed ...$value
     * @return int  length of the list after the push operation.
     */
    abstract public function lPush(string $key, mixed ...$value): int;

    /**
     * Each element is inserted to the tail of the list, from the leftmost to the rightmost element.
     * Ex: `$client->rPush('mylist', 'a', 'b', 'c')` will create a list `["a", "b", "c"]`.
     *
     * @link https://redis.io/docs/commands/rpush
     * @param string $key
     * @param mixed ...$value
     * @return int  length of the list after the push operation.
     */
    abstract public function rPush(string $key, mixed ...$value): int;

    # endregion LIST ---------------------------------------------------------------------------------------------------

    # region SCRIPT ----------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/commands/eval
     * @param string $script
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    abstract public function eval(string $script, int $numKeys = 0, int|string ...$arg): mixed;

    /**
     * @link https://redis.io/commands/evalsha_ro
     * @param string $script
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    abstract public function evalRo(string $script, int $numKeys = 0, int|string ...$arg): mixed;

    /**
     * @link https://redis.io/commands/evalsha
     * @param string $sha1
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    abstract public function evalSha(string $sha1, int $numKeys = 0, int|string ...$arg): mixed;

    /**
     * @link https://redis.io/commands/evalsha_ro
     * @param string $sha1
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    abstract public function evalShaRo(string $sha1, int $numKeys = 0, int|string ...$arg): mixed;

    /**
     * @link https://redis.io/commands/script-exists
     * @param string ...$sha1
     * @return list<bool>
     */
    abstract public function scriptExists(string ...$sha1): array;

    /**
     * @link https://redis.io/commands/script-flush
     * @return void
     */
    abstract public function scriptFlush(): void;

    /**
     * @link https://redis.io/commands/script-load
     * @return string
     * The SHA1 digest of the script added into the script cache.
     */
    abstract public function scriptLoad(string $script): string;

    # endregion SCRIPT -------------------------------------------------------------------------------------------------

    # region SERVER ----------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/acl
     * @param string $operation
     * @param string ...$args
     * @return mixed
     */
    abstract public function acl(string $operation, string ...$args): mixed;

    /**
     * @link https://redis.io/docs/commands/dbsize
     * @return int
     */
    abstract public function dbSize(): int;

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
    abstract public function xAdd(string $key, string $id, iterable $fields, ?int $maxLen = null, bool $approximate = false): string;

    /**
     * @link https://redis.io/docs/commands/xdel
     * @param string $key
     * @param string ...$id
     * @return int
     * The number of entries deleted.
     */
    abstract public function xDel(string $key, string ...$id): int;

    /**
     * @link https://redis.io/docs/commands/xlen
     * @param string $key
     * @return int
     */
    abstract public function xLen(string $key): int;

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
    abstract public function xRange(string $key, string $start, string $end, ?int $count = null): array;

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
    abstract public function xRead(iterable $streams, ?int $count = null, ?int $blockMilliseconds = null): array;

    /**
     * @link https://redis.io/docs/commands/xrevrange
     * @param string $key
     * @param string $end
     * @param string $start
     * @param int|null $count
     * @return array<string, array<string, mixed>>
     * A list of stream entries with IDs matching the specified range.
     */
    abstract public function xRevRange(string $key, string $end, string $start, ?int $count = null): array;

    /**
     * @link https://redis.io/docs/commands/xtrim
     * @param string $key
     * @param int|string $threshold
     * @param int|null $limit
     * @param XtrimMode $mode
     * @param bool $approximate
     * @return int
     * The number of entries deleted.
     */
    abstract public function xTrim(string $key, int|string $threshold, ?int $limit = null, XtrimMode $mode = XtrimMode::MaxLen, bool $approximate = false): int;

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
    abstract public function decr(string $key, int $by = 1): int;

    /**
     * @link https://redis.io/docs/commands/decrbyfloat
     * @param string $key
     * @param float $by
     * @return float
     * The decremented value
     */
    abstract public function decrByFloat(string $key, float $by): float;

    /**
     * @link https://redis.io/docs/commands/get
     * @param string $key
     * @return mixed|false
     * `false` if key does not exist.
     */
    abstract public function get(string $key): mixed;

    /**
     * @link https://redis.io/docs/commands/getdel
     * @param string $key
     * @return mixed|false
     * `false` if key does not exist.
     */
    abstract public function getDel(string $key): mixed;

    /**
     * @link https://redis.io/docs/commands/incr
     * @link https://redis.io/docs/commands/incrby
     * @param string $key
     * @param int $by
     * @return int
     * The incremented value
     */
    abstract public function incr(string $key, int $by = 1): int;

    /**
     * @link https://redis.io/docs/commands/incrbyfloat
     * @param string $key
     * @param float $by
     * @return float
     * The incremented value
     */
    abstract public function incrByFloat(string $key, float $by): float;

    /**
     * @link https://redis.io/docs/commands/mget
     * @param string ...$key
     * @return array<string, mixed|false>
     * Returns `[{retrieved_key} => value, ...]`. `false` if key is not found.
     */
    abstract public function mGet(string ...$key): array;

    /**
     * @link https://redis.io/docs/commands/mset
     * @param iterable<string, mixed> $pairs
     * @return void
     */
    abstract public function mSet(iterable $pairs): void;

    /**
     * @link https://redis.io/docs/commands/msetnx
     * @param iterable<string, mixed> $pairs
     * @return bool
     */
    abstract public function mSetNx(iterable $pairs): bool;

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
    abstract public function set(
        string $key,
        mixed $value,
        ?SetMode $mode = null,
        ?int $ex = null,
        ?DateTimeInterface $exAt = null,
        bool $keepTtl = false,
        bool $get = false,
    ): mixed;

    # endregion STRING -------------------------------------------------------------------------------------------------
}
