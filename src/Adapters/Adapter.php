<?php declare(strict_types=1);

namespace Kirameki\Redis\Adapters;

use Generator;
use Kirameki\Redis\Config\ConnectionConfig;

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
    abstract public function command(string $name, array $args): mixed;

    /**
     * @param string $name
     * @param array<mixed> $args
     * @return mixed
     */
    abstract public function rawCommand(string $name, array $args): mixed;

    /**
     * @return int
     */
    abstract public function dbSize(): int;

    /**
     * @param string $pattern
     * @param int $count
     * @param bool $prefixed
     * @return Generator<int, string>
     */
    abstract public function scan(string $pattern = '*', int $count = 10_000, bool $prefixed = false): Generator;

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
}
