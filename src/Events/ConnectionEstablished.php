<?php declare(strict_types=1);

namespace Kirameki\Redis\Events;

use Kirameki\Redis\Connection;

class ConnectionEstablished extends RedisEvent
{
    /**
     * @param Connection $connection
     */
    public function __construct(
        Connection $connection,
    )
    {
        parent::__construct($connection);
    }
}
