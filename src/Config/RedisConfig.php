<?php declare(strict_types=1);

namespace Kirameki\Redis\Config;

class RedisConfig
{
    /**
     * @param array<ConnectionConfig> $connections
     */
    public function __construct(
        public array $connections = [],
        public ?string $default = null,
    )
    {
    }
}
