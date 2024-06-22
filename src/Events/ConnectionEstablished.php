<?php declare(strict_types=1);

namespace Kirameki\Redis\Events;

use Kirameki\Redis\RedisConnection;

class ConnectionEstablished extends RedisEvent
{
    /**
     * @param RedisConnection $connection
     */
    public function __construct(
        RedisConnection $connection,
    )
    {
        parent::__construct($connection);
    }
}
