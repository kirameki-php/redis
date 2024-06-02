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
    abstract public function isConnected(): bool;

    /**
     * @param string $name
     * @param mixed ...$args
     * @return mixed
     */
    abstract public function command(string $name, mixed ...$args): mixed;

    /**
     * @param string|null $pattern
     * @param int $count
     * @param bool $prefixed
     * @return Generator<int, string>
     */
    abstract public function scan(string $pattern = null, int $count = 0, bool $prefixed = false): Generator;
}
