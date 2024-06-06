<?php declare(strict_types=1);

namespace Kirameki\Redis\Adapters;

use Generator;
use Kirameki\Redis\Config\ConnectionConfig;
use function dump;

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
     * @param list<mixed> $args
     * @return mixed
     */
    abstract public function command(string $name, array $args): mixed;

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
}
