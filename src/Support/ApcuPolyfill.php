<?php

if (!function_exists('apcu_fetch')) {
    /**
     * @param mixed $key
     * @param bool|null $success
     */
    function apcu_fetch(mixed $key, ?bool &$success = null): mixed
    {
        $success = false;

        return false;
    }
}

if (!function_exists('apcu_add')) {
    /**
     * @param mixed $key
     * @param mixed $var
     */
    function apcu_add(mixed $key, mixed $var = null, int $ttl = 0): bool
    {
        return false;
    }
}
