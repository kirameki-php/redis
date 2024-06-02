<?php declare(strict_types=1);

namespace Kirameki\Redis;

use Closure;
use Kirameki\Collections\Map;
use Kirameki\Core\Exceptions\LogicException;
use Kirameki\Event\EventManager;
use Kirameki\Redis\Adapters\Adapter;
use Kirameki\Redis\Adapters\ExtensionAdapter;
use Kirameki\Redis\Config\ConnectionConfig;
use Kirameki\Redis\Config\ExtensionConfig;
use Kirameki\Redis\Config\RedisConfig;
use function array_key_exists;

class RedisManager
{
    /**
     * @var array<string, Connection>
     */
    protected array $connections = [];

    /**
     * @var array<string, Closure(ConnectionConfig): Adapter<ConnectionConfig>>
     */
    protected array $adapters = [];

    /**
     * @var string
     */
    public readonly string $default;

    /**
     * @param EventManager $events
     * @param RedisConfig $config
     */
    public function __construct(
        protected readonly EventManager $events,
        public readonly RedisConfig $config,
    )
    {
        $this->default = $this->resolveDefaultConnectionName();
    }

    /**
     * @param string $name
     * @return Connection
     */
    public function use(string $name): Connection
    {
        return $this->connections[$name] ??= $this->createConnection($name);
    }

    /**
     * @return Connection
     */
    public function useDefault(): Connection
    {
        return $this->use($this->default);
    }

    /**
     * @param string $name
     * @return Connection
     */
    protected function createConnection(string $name): Connection
    {
        $config = $this->getConfig($name);
        $resolver = $this->getAdapterResolver($config->getAdapterName());
        return new Connection($name, $resolver($config), $this->events);
    }

    /**
     * @return Map<string, Connection>
     */
    public function resolvedConnections(): Map
    {
        return new Map($this->connections);
    }

    /**
     * @param string $name
     * @return $this
     */
    public function purge(string $name): static
    {
        unset($this->connections[$name]);
        return $this;
    }

    /**
     * @return $this
     */
    public function purgeAll(): static
    {
        $this->connections = [];
        return $this;
    }

    /**
     * @param string $name
     * @return ConnectionConfig
     */
    public function getConfig(string $name): ConnectionConfig
    {
        return $this->config->connections[$name] ?? throw new LogicException("Database: {$name} does not exist", [
            'name' => $name,
            'config' => $this->config,
        ]);
    }

    /**
     * @param string $name
     * @param ConnectionConfig $config
     * @return $this
     */
    public function setConnectionConfig(string $name, ConnectionConfig $config): static
    {
        $this->config->connections[$name] = $config;
        return $this;
    }

    /**
     * @return string
     */
    protected function resolveDefaultConnectionName(): string
    {
        $default = $this->config->default;
        if ($default !== null) {
            return $default;
        }
        $connections = $this->config->connections;
        if (count($connections) === 1) {
            return array_key_first($connections);
        }
        throw new LogicException('No default connection could be resolved', [
            'config' => $this->config,
        ]);
    }

    /**
     * @param string $name
     * @param Closure(ConnectionConfig): Adapter<ConnectionConfig> $deferred
     * @return $this
     */
    public function addAdapter(string $name, Closure $deferred): static
    {
        $this->adapters[$name] = $deferred(...);
        return $this;
    }

    /**
     * @param string $name
     * @return Closure(ConnectionConfig): Adapter<ConnectionConfig>
     */
    protected function getAdapterResolver(string $name): Closure
    {
        if (!array_key_exists($name, $this->adapters)) {
            $this->addAdapter($name, $this->getDefaultAdapterResolver($name));
        }
        return $this->adapters[$name];
    }

    /**
     * @param string $name
     * @return Closure(covariant ConnectionConfig): Adapter<ConnectionConfig>
     */
    protected function getDefaultAdapterResolver(string $name): Closure
    {
        return match ($name) {
            'redis' => static fn(ExtensionConfig $config) => new ExtensionAdapter($config),
            default => throw new LogicException("Adapter: $name does not exist"),
        };
    }
}
