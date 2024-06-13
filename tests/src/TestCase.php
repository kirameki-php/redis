<?php declare(strict_types=1);

namespace Tests\Kirameki\Redis;

use Kirameki\Core\Testing\TestCase as BaseTestCase;
use Kirameki\Event\EventManager;
use Kirameki\Redis\Config\ExtensionConfig;
use Kirameki\Redis\Config\RedisConfig;
use Kirameki\Redis\Connection;
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
        return $this->redis ??= new RedisManager(new EventManager(), new RedisConfig(default: 'main'));
    }

    public function createExtConnection(string $name, ?ExtensionConfig $config = null): Connection
    {
        $redis = $this->createManager();
        $redis->setConnectionConfig($name, $config ?? new ExtensionConfig('redis', serializer: Redis::SERIALIZER_PHP));

        $connection = $redis->use($name);
        $this->runAfterTearDown(static function () use ($connection): void {
            if ($connection->isConnected()) {
                $connection->flushKeys();
            }
        });

        return $connection;
    }
}
