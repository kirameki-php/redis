<?php declare(strict_types=1);

namespace Kirameki\Redis\Options;

use DateTimeInterface;

final class SetOptions
{
    /**
     * @param bool $nx
     * @param bool $xx
     * @param int|null $ex
     * @param DateTimeInterface|null $exAt
     * @param bool $keepTtl
     * @param bool $get
     * @return static
     */
    public static function of(
        bool $nx = false,
        bool $xx = false,
        ?int $ex = null,
        ?DateTimeInterface $exAt = null,
        bool $keepTtl = false,
        bool $get = false,
    ): self
    {
        return new self($nx, $xx, $ex, $exAt, $keepTtl, $get);
    }

    /**
     * @param bool $nx
     * @param bool $xx
     * @param int|null $ex
     * @param DateTimeInterface|null $exAt
     * @param bool $keepTtl
     * @param bool $get
     */
    public function __construct(
        protected bool $nx = false,
        protected bool $xx = false,
        protected ?int $ex = null,
        protected ?DateTimeInterface $exAt = null,
        protected bool $keepTtl = false,
        protected bool $get = false,
    )
    {
    }

    /**
     * @return array<int|string, scalar>
     */
    public function toArray(): array
    {
        $options = [];
        if ($this->nx) {
            $options[] = 'nx';
        }
        if ($this->xx) {
            $options[] = 'xx';
        }
        if ($this->ex !== null) {
            $options['ex'] = $this->ex;
        }
        if ($this->exAt !== null) {
            $options['exat'] = $this->exAt->getTimestamp();
        }
        if ($this->keepTtl) {
            $options[] = 'keepttl';
        }
        if ($this->get) {
            $options[] = 'get';
        }
        return $options;
    }
}
