<?php declare(strict_types=1);

namespace Kirameki\Redis\Config;

use Redis;

class ExtensionConfig extends ConnectionConfig
{
    public function __construct(
        string $host = 'localhost',
        int $port = 6379,
        ?string $username = null,
        ?string $password = null,
        bool $persistent = false,
        ?float $connectionTimeoutSeconds = null,
        ?float $readTimeoutSeconds = null,
        string $prefix = '',
        public ?int $database = null,
        public int $serializer = Redis::SERIALIZER_NONE,
    )
    {
        parent::__construct(
            $host,
            $port,
            $username,
            $password,
            $persistent,
            $connectionTimeoutSeconds,
            $readTimeoutSeconds,
            $prefix,
        );
    }

    /**
     * @return string
     */
    public function getAdapterName(): string
    {
        return 'ext-redis';
    }
}
