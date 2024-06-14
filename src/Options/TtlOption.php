<?php declare(strict_types=1);

namespace Kirameki\Redis\Options;

enum TtlOption: string
{
    case Nx = 'nx';
    case Xx = 'xx';
    case Gt = 'gt';
    case Lt = 'lt';
}
