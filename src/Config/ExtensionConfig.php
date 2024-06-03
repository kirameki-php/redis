<?php declare(strict_types=1);

namespace Kirameki\Redis\Config;

use Redis;

class ExtensionConfig extends ConnectionConfig
{
    public function __construct(
        ?string $host = null,
        ?int $port = null,
        ?string $socket = null,
        ?string $username = null,
        ?string $password = null,
        bool $persistent = false,
        ?float $connectTimeoutSeconds = null,
        ?float $readTimeoutSeconds = null,
        string $prefix = '',
        public ?int $database = null,
        public int $serializer = Redis::SERIALIZER_IGBINARY,
    )
    {
        parent::__construct(
            $host,
            $port,
            $socket,
            $username,
            $password,
            $persistent,
            $connectTimeoutSeconds,
            $readTimeoutSeconds,
            $prefix,
        );
    }

    /**
     * @return string
     */
    public function getAdapterName(): string
    {
        return 'extension';
    }
}
