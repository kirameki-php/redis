<?php declare(strict_types=1);

namespace Kirameki\Redis\Events;

use Kirameki\Event\Event;
use Kirameki\Redis\Connection;

abstract class RedisEvent extends Event
{
    /**
     * @param Connection $connection
     */
    public function __construct(
        public readonly Connection $connection,
    )
    {
    }
}
