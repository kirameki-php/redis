<?php declare(strict_types=1);

namespace Kirameki\Redis\Support;

use DateTimeInterface;

class SetOption
{
    /**
     * @var float
     */
    protected float $px;

    /**
     * @var float
     */
    protected float $pxAt;

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
        protected string $set,
    )
    {
    }

    /**
     * @param float $seconds
     * @return $this
     */
    public function expireIn(float $seconds): static
    {
        $this->px = $seconds;
        return $this;
    }

    /**
     * @param float|DateTimeInterface $time
     * @return $this
     */
    public function expireAt(float|DateTimeInterface $time): static
    {
        $this->pxAt = ($time instanceof DateTimeInterface)
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
        $options[] = $this->set;

        if ($this->px) {
            $options['PX'] = $this->px * 1000;
        }

        if ($this->pxAt) {
            $options['PXAT'] = $this->pxAt * 1000;
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
