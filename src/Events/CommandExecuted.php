<?php declare(strict_types=1);

namespace Kirameki\Redis\Events;

use Kirameki\Redis\RedisConnection;

class CommandExecuted extends RedisEvent
{
    /**
     * @param RedisConnection $connection
     * @param string $command
     * @param array<mixed> $args
     * @param mixed $result
     * @param float $elapsedMs
     */
    public function __construct(
        RedisConnection $connection,
        public readonly string $command,
        public readonly array $args,
        public readonly mixed $result,
        public readonly float $elapsedMs,
    )
    {
        parent::__construct($connection);
    }
}
