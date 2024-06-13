<?php declare(strict_types=1);

namespace Kirameki\Redis\Support;

use DateTimeInterface;

class SetOption
{
    /**
     * @var float|null
     */
    protected ?float $ex = null;

    /**
     * @var float|null
     */
    protected ?float $exAt = null;

    /**
     * @var bool|null
     */
    protected ?bool $keepTtl = null;

    /**
     * @var bool|null
     */
    protected ?bool $get = null;

    /**
     * @return self
     */
    public static function ifNotExist(): self
    {
        return new self('NX');
    }

    /**
     * @return self
     */
    public static function ifExist(): self
    {
        return new self('XX');
    }

    /**
     * @param string $set
     */
    public function __construct(
        protected ?string $set = null,
    )
    {
    }

    /**
     * @param float $seconds
     * @return $this
     */
    public function expireIn(float $seconds): static
    {
        $this->ex = $seconds;
        return $this;
    }

    /**
     * @param float|DateTimeInterface $time
     * @return $this
     */
    public function expireAt(float|DateTimeInterface $time): static
    {
        $this->exAt = ($time instanceof DateTimeInterface)
            ? (float) $time->format('U.u')
            : $time;
        return $this;
    }

    /**
     * @param bool $toggle
     * @return $this
     */
    public function keepTtl(bool $toggle = true): static
    {
        $this->keepTtl = $toggle;
        return $this;
    }

    /**
     * @param bool $toggle
     * @return $this
     */
    public function get(bool $toggle = true): static
    {
        $this->get = $toggle;
        return $this;
    }

    /**
     * @return array<int|string, scalar>
     */
    public function toArray(): array
    {
        $options = [];

        if ($this->set !== null) {
            $options[] = $this->set;
        }

        if ($this->ex !== null) {
            $options['PX'] = $this->ex * 1000;
        }

        if ($this->exAt !== null) {
            $options['PXAT'] = $this->exAt * 1000;
        }

        if ($this->keepTtl !== null) {
            $options[] = 'KEEPTTL';
        }

        if ($this->get !== null) {
            $options[] = 'GET';
        }

        return $options;
    }
}
