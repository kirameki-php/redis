<?php declare(strict_types=1);

namespace Kirameki\Redis\Adapters;

use DateTimeInterface;
use Generator;
use Kirameki\Redis\Config\ConnectionConfig;
use Kirameki\Redis\Exceptions\CommandException;
use Kirameki\Redis\Options\SetMode;
use Kirameki\Redis\Options\TtlMode;
use Kirameki\Redis\Options\Type;
use Kirameki\Redis\Options\XTrimMode;

/**
 * @template TConnectionConfig of ConnectionConfig
 */
interface Adapter
{
    /**
     * @return $this
     */
    public function connect(): static;

    /**
     * @return bool
     */
    public function disconnect(): bool;

    /**
     * @return bool
     */
    public function isConnected(): bool;

    # region CONNECTION ------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/client-id
     * @return int
     */
    public function clientId(): int;

    /**
     * @link https://redis.io/docs/commands/client-info
     * @return array<string, ?scalar>
     */
    public function clientInfo(): array;

    /**
     * @link https://redis.io/docs/commands/client-kill
     * @param string $ipAddressAndPort
     * @return bool
     */
    public function clientKill(string $ipAddressAndPort): bool;

    /**
     * @link https://redis.io/docs/commands/client-list
     * @return list<array{ id: int, addr: string, laddr: string, fd: int, name: string, db: int }>
     */
    public function clientList(): array;

    /**
     * @link https://redis.io/docs/commands/client-getname
     * @return string|null
     */
    public function clientGetname(): ?string;

    /**
     * @link https://redis.io/docs/commands/client-setname
     * @param string $name
     * @return void
     */
    public function clientSetname(string $name): void;

    /**
     * @link https://redis.io/docs/commands/echo
     * @param string $message
     * @return string
     */
    public function echo(string $message): string;

    /**
     * @link https://redis.io/docs/commands/ping
     * @return bool
     */
    public function ping(): bool;

    # endregion CONNECTION ---------------------------------------------------------------------------------------------

    # region GENERIC ---------------------------------------------------------------------------------------------------

    /**
     * @param string $name
     * @param array<mixed> $args
     * @return mixed
     */
    public function rawCommand(string $name, array $args): mixed;

    /**
     * @link https://redis.io/docs/commands/del
     * @param string ...$key
     * @return int
     * Returns the number of keys that were removed.
     */
    public function del(string ...$key): int;

    /**
     * @link https://redis.io/docs/commands/exists
     * @param string ...$key
     * @return int
     */
    public function exists(string ...$key): int;

    /**
     * @link https://redis.io/docs/commands/expiretime
     * @param string $key
     * @return int|false|null
     * Returns the remaining time to live of a key that has a timeout.
     * Returns `null` if key exists but has no associated expire.
     * Returns `false` if key does not exist.
     */
    public function expireTime(string $key): int|false|null;

    /**
     * @link https://redis.io/docs/commands/pexpiretime
     * @param string $key
     * @return int|false|null
     * Returns the remaining time to live of a key that has a timeout.
     * Returns `null` if key exists but has no associated expire.
     * Returns `false` if key does not exist.
     */
    public function pExpireTime(string $key): int|false|null;

    /**
     * @link https://redis.io/docs/commands/perist
     * @param string $key
     * @return bool
     */
    public function persist(string $key): bool;

    /**
     * @link https://redis.io/docs/commands/expire
     * @param string $key
     * @param int $seconds
     * @param TtlMode|null $mode
     * @return bool
     */
    public function expire(string $key, int $seconds, ?TtlMode $mode = null): bool;

    /**
     * @link https://redis.io/docs/commands/pexpire
     * @param string $key
     * @param int $milliseconds
     * @param TtlMode|null $mode
     * @return bool
     */
    public function pExpire(string $key, int $milliseconds, ?TtlMode $mode = null): bool;

    /**
     * @link https://redis.io/docs/commands/expireat
     * @param string $key
     * @param DateTimeInterface $time
     * @param TtlMode|null $mode
     * @return bool
     */
    public function expireAt(string $key, DateTimeInterface $time, ?TtlMode $mode = null): bool;

    /**
     * @link https://redis.io/docs/commands/pexpireat
     * @param string $key
     * @param DateTimeInterface $time
     * @param TtlMode|null $mode
     * @return bool
     */
    public function pExpireAt(string $key, DateTimeInterface $time, ?TtlMode $mode = null): bool;

    /**
     * @link https://redis.io/docs/commands/randomkey
     * @return string|null
     * Returns random key existing in server. Returns `null` if no key exists.
     */
    public function randomKey(): ?string;

    /**
     * @link https://redis.io/docs/commands/rename
     * @param string $srcKey
     * @param string $dstKey
     * @return bool
     * `true` in case of success, `false` in case of failure
     * @throws CommandException
     * "ERR no such key" is thrown if no key exists.
     */
    public function rename(string $srcKey, string $dstKey): bool;

    /**
     * @link https://redis.io/docs/commands/renamenx
     * @param string $srcKey
     * @param string $dstKey
     * @return bool
     * `true` in case of success, `false` in case of failure
     * @throws CommandException
     * "ERR no such key" is thrown if no key exists.
     */
    public function renameNx(string $srcKey, string $dstKey): bool;

    /**
     * @link https://redis.io/docs/commands/scan
     * @param string $pattern
     * @param int $count
     * @param bool $prefixed
     * @return Generator<int, string>
     */
    public function scan(string $pattern = '*', int $count = 10_000, bool $prefixed = false): Generator;

    /**
     * @link https://redis.io/docs/commands/ttl
     * @param string $key
     * @return int|false|null
     * Returns the remaining time to live of a key that has a timeout.
     * Returns `null` if key exists but has no associated expire.
     * Returns `false` if key does not exist.
     */
    public function ttl(string $key): int|false|null;

    /**
     * @link https://redis.io/docs/commands/pttl
     * @param string $key
     * @return int|false|null
     * Returns the remaining time to live of a key that has a timeout.
     * Returns `null` if key exists but has no associated expire.
     * Returns `false` if key does not exist.
     */
    public function pTtl(string $key): int|false|null;

    /**
     * @link https://redis.io/docs/commands/type
     * @param string $key
     * @return Type
     */
    public function type(string $key): Type;

    /**
     * @link https://redis.io/docs/commands/unlink
     * @param string ...$key
     * @return int
     */
    public function unlink(string ...$key): int;

    # endregion GENERIC ------------------------------------------------------------------------------------------------

    # region LIST ------------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/blpop
     * @param list<string> $keys
     * @param int|float $timeout  If no timeout is set, it will be set to 0 which is infinity.
     * @return array<string, mixed>|null  Returns null on timeout
     */
    public function blPop(array $keys, int|float $timeout = 0): ?array;

    /**
     * @link https://redis.io/docs/commands/lindex
     * @param string $key
     * @param int $index  Zero based. Use negative indices to designate elements starting at the tail of the list.
     * @return mixed|false  The value at index or `false` if... (1) key is missing or (2) index is missing.
     * @throws CommandException  if key set but is not a list.
     */
    public function lIndex(string $key, int $index): mixed;

    /**
     * @link https://redis.io/docs/commands/llen
     * @param string $key
     * @return int
     *  The length of the list. 0 if the list does not exist.
     *  Will throw CommandException if key set but is not a list.
     */
    public function lLen(string $key): int;

    /**
     * @link https://redis.io/docs/commands/lpop
     * @param string $key
     * @return mixed|false  The value at the head of the list or `false` if the list is empty or does not exist.
     */
    public function lPop(string $key): mixed;

    /**
     * Each element is inserted to the head of the list, from the leftmost to the rightmost element.
     * Ex: `$client->lPush('mylist', 'a', 'b', 'c')` will create a list `["c", "b", "a"]`
     *
     * @link https://redis.io/docs/commands/lpush
     * @param string $key
     * @param mixed ...$value
     * @return int  length of the list after the push operation.
     */
    public function lPush(string $key, mixed ...$value): int;

    /**
     * @link https://redis.io/docs/commands/lpushx
     * @param string $key
     * @param mixed $value
     * @return int  length of the list after the push operation.
     * Returns `0` if the key does not exist.
     */
    public function lPushx(string $key, mixed $value): int;

    /**
     * @link https://redis.io/docs/commands/lrem
     * @param string $key
     * @param mixed $value  The value to remove from the list.
     * @param int $count
     * The number of occurrences to remove.
     * If positive, it will remove from the head of the list.
     * If negative, it will remove from the tail of the list.
     * If `0`, all occurrences will be removed.
     * @return int  The number of elements removed from the list.
     */
    public function lRem(string $key, mixed $value, int $count): int;

    /**
     * @link https://redis.io/docs/commands/lrange
     * @param string $key
     * @param int $start  Can be negative to designate elements starting at the tail of the list.
     * @param int $end  Can be negative to designate elements starting at the tail of the list.
     * @return list<mixed>  List of elements in the specified range.
     */
    public function lRange(string $key, int $start, int $end): array;

    /**
     * @link https://redis.io/docs/commands/lset
     * @param string $key
     * @param int $index  Zero based. Use negative indices to designate elements starting at the tail of the list.
     * @param mixed $value  The value to set at the specified index.
     * @return bool
     * `true` if the value was set successfully, `false` if the index is out of range or if the key is not a list.
     */
    public function lSet(string $key, int $index, mixed $value): bool;

    /**
     * @link https://redis.io/docs/commands/ltrim
     * @param string $key
     * @param int $start  Can be negative to designate elements starting at the tail of the list.
     * @param int $end  Can be negative to designate elements starting at the tail of the list.
     * @return void
     */
    public function lTrim(string $key, int $start, int $end): void;

    /**
     * @link https://redis.io/docs/commands/rpop
     * @param string $key
     * @return mixed|false  The value at the tail of the list or `false` if the list is empty or does not exist.
     */
    public function rPop(string $key): mixed;

    /**
     * @link https://redis.io/docs/commands/rpoplpush
     * @param string $srcKey  The source key to pop from.
     * @param string $dstKey  The destination key to push to.
     * @return mixed|false
     * The value at the tail of the source list after popping it.
     * `false` if the source list is empty or does not exist.
     */
    public function rPopLPush(string $srcKey, string $dstKey): mixed;

    /**
     * Each element is inserted to the tail of the list, from the leftmost to the rightmost element.
     * Ex: `$client->rPush('mylist', 'a', 'b', 'c')` will create a list `["a", "b", "c"]`.
     *
     * @link https://redis.io/docs/commands/rpush
     * @param string $key
     * @param mixed ...$value
     * @return int  length of the list after the push operation.
     */
    public function rPush(string $key, mixed ...$value): int;

    /**
     * @link https://redis.io/docs/commands/rpushx
     * @param string $key
     * @param mixed $value
     * @return int  length of the list after the push operation.
     * Returns `0` if the key does not exist.
     */
    public function rPushx(string $key, mixed $value): int;

    # endregion LIST ---------------------------------------------------------------------------------------------------

    # region SCRIPT ----------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/commands/eval
     * @param string $script
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    public function eval(string $script, int $numKeys = 0, int|string ...$arg): mixed;

    /**
     * @link https://redis.io/commands/evalsha_ro
     * @param string $script
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    public function evalRo(string $script, int $numKeys = 0, int|string ...$arg): mixed;

    /**
     * @link https://redis.io/commands/evalsha
     * @param string $sha1
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    public function evalSha(string $sha1, int $numKeys = 0, int|string ...$arg): mixed;

    /**
     * @link https://redis.io/commands/evalsha_ro
     * @param string $sha1
     * @param int $numKeys
     * @param int|string ...$arg
     * @return mixed
     */
    public function evalShaRo(string $sha1, int $numKeys = 0, int|string ...$arg): mixed;

    /**
     * @link https://redis.io/commands/script-exists
     * @param string ...$sha1
     * @return list<bool>
     */
    public function scriptExists(string ...$sha1): array;

    /**
     * @link https://redis.io/commands/script-flush
     * @return void
     */
    public function scriptFlush(): void;

    /**
     * @link https://redis.io/commands/script-load
     * @return string
     * The SHA1 digest of the script added into the script cache.
     */
    public function scriptLoad(string $script): string;

    # endregion SCRIPT -------------------------------------------------------------------------------------------------

    # region SERVER ----------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/acl
     * @param string $operation
     * @param string ...$args
     * @return mixed
     */
    public function acl(string $operation, string ...$args): mixed;

    /**
     * @link https://redis.io/docs/commands/dbsize
     * @return int
     */
    public function dbSize(): int;

    # endregion SERVER -------------------------------------------------------------------------------------------------

    # region STREAM ----------------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/xadd
     * @param string $key
     * @param string $id
     * @param array<string, mixed> $fields
     * @param int|null $maxLen
     * @param bool $approximate
     * @return string
     */
    public function xAdd(string $key, string $id, array $fields, ?int $maxLen = null, bool $approximate = false): string;

    /**
     * @link https://redis.io/docs/commands/xclaim
     * @param string $key
     * @param string $group
     * @param string $consumer
     * @param int $minIdleTime
     * @param list<string> $ids
     * @return array<string, mixed>
     */
    public function xClaim(string $key, string $group, string $consumer, int $minIdleTime, array $ids): array;

    /**
     * @link https://redis.io/docs/commands/xdel
     * @param string $key
     * @param string ...$id
     * @return int
     * The number of entries deleted.
     */
    public function xDel(string $key, string ...$id): int;

    /**
     * @link https://redis.io/docs/commands/xinfo-stream
     * @param string $key
     * @param bool $full
     * @param int|null $count
     * limit the number of stream and PEL entries that are returned.
     * The default COUNT is 10 and COUNT of 0 means all entries will be returned.
     * @return array<string, mixed>
     */
    public function xInfoStream(string $key, bool $full = false, ?int $count = null): array;

    /**
     * @link https://redis.io/docs/commands/xlen
     * @param string $key
     * @return int
     */
    public function xLen(string $key): int;

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
    public function xRange(string $key, string $start, string $end, ?int $count = null): array;

    /**
     * [!NOTE] When `blockMilliseconds` > 0, the method will return `null` on timeout.
     * [!WARNING] The process will not respond to any signals until the block timeout is reached.
     *
     * @link https://redis.io/docs/commands/xread
     * @param array<string, string> $streams
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
    public function xRead(array $streams, ?int $count = null, ?int $blockMilliseconds = null): array;

    /**
     * @link https://redis.io/docs/commands/xrevrange
     * @param string $key
     * @param string $end
     * @param string $start
     * @param int|null $count
     * @return array<string, array<string, mixed>>
     * A list of stream entries with IDs matching the specified range.
     */
    public function xRevRange(string $key, string $end, string $start, ?int $count = null): array;

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
    public function xTrim(string $key, int|string $threshold, ?int $limit = null, XTrimMode $mode = XTrimMode::MaxLen, bool $approximate = false): int;

    # endregion STREAM -------------------------------------------------------------------------------------------------

    # region STREAM GROUP-----------------------------------------------------------------------------------------------

    /**
     * @link https://redis.io/docs/commands/xack
     * @param string $key
     * @param string $group
     * @param list<string> $ids
     * @return int
     * The number of successfully acknowledged messages.
     */
    public function xAck(string $key, string $group, array $ids): int;

    /**
     * @link https://redis.io/docs/commands/xgroup-create
     * @param string $key
     * @param string $group
     * @param string $id
     * @param bool $mkStream
     * @return void
     */
    public function xGroupCreate(string $key, string $group, string $id, bool $mkStream = false): void;

    /**
     * @link https://redis.io/docs/commands/xgroup-createconsumer
     * @param string $key
     * @param string $group
     * @param string $consumer
     * @return int<0, 1>
     * The number of created consumers, either 0 or 1.
     */
    public function xGroupCreateConsumer(string $key, string $group, string $consumer): int;

    /**
     * @link https://redis.io/docs/commands/xgroup-delconsumer
     * @param string $key
     * @param string $group
     * @param string $consumer
     * @return int
     * The number of pending messages the consumer had before it was deleted.
     *  If the consumer does not exist, 0 is returned.
     */
    public function xGroupDelConsumer(string $key, string $group, string $consumer): int;

    /**
     * @link https://redis.io/docs/commands/xgroup-destroy
     * @param string $key
     * @param string $group
     * @return int<0, 1>
     * the number of destroyed consumer groups, either 0 or 1.
     */
    public function xGroupDestroy(string $key, string $group): int;

    /**
     * @link https://redis.io/docs/commands/xgroup-setid
     * @param string $key
     * @param string $group
     * @param string $id
     * @return void
     */
    public function xGroupSetId(string $key, string $group, string $id): void;

    /**
     * @link https://redis.io/docs/commands/xinfo-consumers
     * @param string $key
     * @param string $group
     * @return list<array<string, scalar>>
     */
    public function xInfoConsumers(string $key, string $group): array;

    /**
     * @link https://redis.io/docs/commands/xinfo-groups
     * @param string $key
     * @return list<array<string, scalar>>
     */
    public function xInfoGroups(string $key): array;

    /**
     * @link https://redis.io/docs/commands/xreadgroup
     * @param string $group
     * @param string $consumer
     * @param array<string, string> $streams
     * @param int|null $count
     * @param int|null $blockMilliseconds
     * @return array<string, array<string, mixed>>
     */
    public function xReadGroup(string $group, string $consumer, array $streams, ?int $count = null, ?int $blockMilliseconds = null): array;

    /**
     * @link https://redis.io/docs/commands/xpending
     * @param string $key
     * @param string $group
     * @param string|null $start
     * @param string|null $end
     * @param int|null $count
     * @param string|null $consumer
     * @return array<string, mixed>
     */
    public function xPending(string $key, string $group, ?string $start = null, ?string $end = null, ?int $count = null, ?string $consumer = null): array;

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
    public function decr(string $key, int $by = 1): int;

    /**
     * @link https://redis.io/docs/commands/decrbyfloat
     * @param string $key
     * @param float $by
     * @return float
     * The decremented value
     */
    public function decrByFloat(string $key, float $by): float;

    /**
     * @link https://redis.io/docs/commands/get
     * @param string $key
     * @return mixed|false
     * `false` if key does not exist.
     */
    public function get(string $key): mixed;

    /**
     * @link https://redis.io/docs/commands/getdel
     * @param string $key
     * @return mixed|false
     * `false` if key does not exist.
     */
    public function getDel(string $key): mixed;

    /**
     * @link https://redis.io/docs/commands/incr
     * @link https://redis.io/docs/commands/incrby
     * @param string $key
     * @param int $by
     * @return int
     * The incremented value
     */
    public function incr(string $key, int $by = 1): int;

    /**
     * @link https://redis.io/docs/commands/incrbyfloat
     * @param string $key
     * @param float $by
     * @return float
     * The incremented value
     */
    public function incrByFloat(string $key, float $by): float;

    /**
     * @link https://redis.io/docs/commands/mget
     * @param string ...$key
     * @return array<string, mixed|false>
     * Returns `[{retrieved_key} => value, ...]`. `false` if key is not found.
     */
    public function mGet(string ...$key): array;

    /**
     * @link https://redis.io/docs/commands/mset
     * @param array<string, mixed> $pairs
     * @return void
     */
    public function mSet(array $pairs): void;

    /**
     * @link https://redis.io/docs/commands/msetnx
     * @param array<string, mixed> $pairs
     * @return bool
     */
    public function mSetNx(array $pairs): bool;

    /**
     * @link https://redis.io/docs/commands/set
     * @param string $key
     * The key to set.
     * @param mixed $value
     * The value to set. Can be any type when serialization is enabled, can only be scalar type when disabled.
     * @param SetMode|null $mode
     * The mode to set the key. Can be `SetMode::Nx` or `SetMode::Xx`. Defaults to `null`.
     * @param int|null $ex
     * The number of seconds until the key will expire. Can not be used with `px`, `exAt`.
     * Defaults to `null`.
     * @param DateTimeInterface|null $exAt
     * * The timestamp when the key will expire. Can not be used with `ex`, `px`, `pxAt`.
     * @param int|null $px
     * *  The number of milliseconds until the key will expire. Can not be used with `ex`, `exAt`, or `pxAt`.
     * * Defaults to `null`.
     * @param DateTimeInterface|null $pxAt
     * * The timestamp when the key will expire. Can not be used with `ex`, `px`, `exAt`.
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
        ?int $px = null,
        ?DateTimeInterface $pxAt = null,
        bool $keepTtl = false,
        bool $get = false,
    ): mixed;

    # endregion STRING -------------------------------------------------------------------------------------------------
}
