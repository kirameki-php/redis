<?php declare(strict_types=1);

namespace Kirameki\Redis\Support;

use DateTimeInterface;

class SetOption
{
    /**
     * @var float
     */
    protected float $ex;

    /**
     * @var float
     */
    protected float $exAt;

    /**
     * @var bool
     */
    protected bool $keepTtl;

    /**
     * @var bool
     */
    protected bool $get;

    /**
     * @return self
     */
    public static function notAlreadyExist(): self
    {
        return new self('NX');
    }

    /**
     * @return self
     */
    public static function alreadyExist(): self
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

        if ($this->ex) {
            $options['PX'] = $this->ex * 1000;
        }

        if ($this->exAt) {
            $options['PXAT'] = $this->exAt * 1000;
        }

        if ($this->keepTtl) {
            $options[] = 'KEEPTTL';
        }

        if ($this->get) {
            $options[] = 'GET';
        }

        return $options;
    }
}
