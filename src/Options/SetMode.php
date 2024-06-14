<?php declare(strict_types=1);

namespace Kirameki\Redis\Options;

enum SetMode: string
{
    case Nx = 'nx';
    case Xx = 'xx';
}
