<?php declare(strict_types=1);

namespace Tests\Kirameki\Redis;

use Kirameki\Core\Testing\TestCase as BaseTestCase;
use Kirameki\Event\EventManager;
use Kirameki\Redis\Config\PhpRedisConfig;
use Kirameki\Redis\Config\RedisConfig;
use Kirameki\Redis\RedisConnection;
use Kirameki\Redis\RedisManager;
use Redis;

/**
 * @mixin TestCase
 */
abstract class TestCase extends BaseTestCase
{
    protected ?RedisManager $redis = null;

    protected function createManager(): RedisManager
    {
        return $this->redis ??= new RedisManager(new RedisConfig(default: 'main'), new EventManager());
    }

    public function createExtConnection(string $name, ?PhpRedisConfig $config = null): RedisConnection
    {
        $redis = $this->createManager();
        $redis->setConnectionConfig($name, $config ?? new PhpRedisConfig('redis'));

        $connection = $redis->use($name);
        $this->runAfterTearDown(static function () use ($connection): void {
            if ($connection->isConnected()) {
                $connection->flushKeys();
            }
        });

        return $connection;
    }
}
