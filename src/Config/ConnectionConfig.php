<?php declare(strict_types=1);

namespace Kirameki\Redis\Config;

abstract class ConnectionConfig
{
    /**
     * @param string|null $host
     * @param int|null $port
     * @param string|null $socket,
     * @param string|null $username
     * @param string|null $password
     * @param bool $persistent
     * @param float|null $connectTimeoutSeconds
     * @param float|null $readTimeoutSeconds
     * @param string $prefix
     */
    public function __construct(
        public ?string $host = null,
        public ?int $port = null,
        public ?string $socket = null,
        public ?string $username = null,
        public ?string $password = null,
        public bool $persistent = false,
        public ?float $connectTimeoutSeconds = null,
        public ?float $readTimeoutSeconds = null,
        public string $prefix = '',
    )
    {
    }

    /**
     * @return string
     */
    abstract public function getAdapterName(): string;
}
