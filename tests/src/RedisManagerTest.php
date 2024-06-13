<?php declare(strict_types=1);

namespace Tests\Kirameki\Redis;

use Kirameki\Event\EventManager;
use Kirameki\Redis\Config\ExtensionConfig;
use Kirameki\Redis\Config\RedisConfig;
use Kirameki\Redis\Connection;
use Kirameki\Redis\Exceptions\ConnectionException;
use Kirameki\Redis\RedisManager;
use LogicException;
use PHPUnit\Framework\Attributes\WithoutErrorHandler;

final class RedisManagerTest extends TestCase
{
    public function test__auto_detect_default(): void
    {
        $manager = new RedisManager(new EventManager(), new RedisConfig(
            connections: [
                'main' => new ExtensionConfig('redis'),
            ]
        ));
        $this->assertSame('main', $manager->default);
    }

    public function test__auto_detect_default_fail(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('No default connection could be resolved.');
        new RedisManager(new EventManager(), new RedisConfig(
            connections: [
                'main' => new ExtensionConfig('redis'),
                'alt' => new ExtensionConfig('redis'),
            ]
        ));
    }

    public function test_use__main(): void
    {
        $this->createExtConnection('main');
        $this->assertInstanceOf(Connection::class, $this->createManager()->use('main'));
    }

    #[WithoutErrorHandler]
    public function test_use__non_existing_name(): void
    {
        $this->throwOnError();
        $this->expectException(ConnectionException::class);
        $this->expectExceptionMessage('php_network_getaddresses: getaddrinfo for ng failed:');
        $this->createExtConnection('ng', new ExtensionConfig('ng'))->exists('a');
    }

    public function test_use__connect_to_bad_host(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Database: ng does not exist');
        $this->createManager()->use('ng')->exists('a');
    }

    public function test_useDefault(): void
    {
        $this->createExtConnection('main');
        $this->assertInstanceOf(Connection::class, $this->createManager()->useDefault());
    }

    public function test_purge(): void
    {
        $manager = $this->createManager();
        $this->createExtConnection('main');
        $this->assertSame(['main'], $manager->resolvedConnections()->keys()->all());
        $manager->purge('main');
        $this->assertSame([], $manager->resolvedConnections()->keys()->all());
    }

    public function test_purge__non_existing_name(): void
    {
        $this->expectException(LogicException::class);
        $this->expectExceptionMessage('Failed to purge connection: "nil" does not exist');
        $this->createExtConnection('main');
        $this->createManager()->purge('nil');
    }

    public function test_purgeAll(): void
    {
        $this->createExtConnection('main');
        $this->createExtConnection('alt');
        $manager = $this->createManager();
        $this->assertSame(['main', 'alt'], $manager->resolvedConnections()->keys()->all());
        $this->assertInstanceOf(RedisManager::class, $manager->purgeAll());
        $this->assertSame([], $manager->resolvedConnections()->all());
    }
}
