<?php declare(strict_types=1);

namespace Kirameki\Redis\Adapters;

use Kirameki\Redis\Config\PhpRedisConfig;
use Redis;
use function assert;

/**
 * @extends Adapter<PhpRedisConfig>
 */
class PhpRedisAdapter extends Adapter
{
    /**
     * @return Redis
     */
    public function getConnectedClient(): object
    {
        if ($this->redis === null) {
            $config = $this->config;
            $this->redis = $this->connectDirect(
                $config->host,
                $config->port,
                $config->persistent,
            );

            if ($config->database !== null) {
                $this->redis->select($config->database);
            }
        }
        assert($this->redis instanceof Redis);
        return $this->redis;
    }

    /**
     * @return list<Redis>
     */
    public function connectToNodes(): array
    {
        $config = $this->config;
        $host = $config->host;
        $port = $config->port;

        return [
            $this->connectDirect($host, $port, false),
        ];
    }
}
