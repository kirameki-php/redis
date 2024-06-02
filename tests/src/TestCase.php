<?php declare(strict_types=1);

namespace Tests\Kirameki\Redis;

use Kirameki\Event\EventManager;
use Kirameki\Redis\Config\ExtensionConfig;
use Kirameki\Redis\Config\RedisConfig;
use Kirameki\Redis\Connection;
use Kirameki\Redis\RedisManager;

/**
 * @mixin TestCase
 */
class TestCase extends \Kirameki\Core\Testing\TestCase
{
    public function createRedisConnection(string $name): Connection
    {
        $events = new EventManager();
        $config = new RedisConfig(
            connections: [
                new ExtensionConfig(
                    host: 'redis',
                ),
            ],
        );
        $redis = new RedisManager($events, $config);
        $connection = $redis->use($name);

        $this->runAfterTearDown(static function () use ($connection): void {
            if ($connection->isConnected()) {
                $connection->flushKeys();
                $connection->select(0);
            }
        });

        return $connection;
    }
}
