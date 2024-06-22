<?php declare(strict_types=1);

namespace Kirameki\Redis\Events;

use Kirameki\Event\Event;
use Kirameki\Redis\RedisConnection;

abstract class RedisEvent extends Event
{
    /**
     * @param RedisConnection $connection
     */
    public function __construct(
        public readonly RedisConnection $connection,
    )
    {
    }
}
