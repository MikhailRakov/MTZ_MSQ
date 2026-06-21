#!/bin/bash
set -e

# Создаём директорию приложения, если она не смонтирована
mkdir -p /app/AP3DMT_app/application

# Настраиваем переменные окружения для MATLAB Runtime
export MCR_ROOT=/opt/matlab
export LD_LIBRARY_PATH=${MCR_ROOT}/runtime/glnxa64:${MCR_ROOT}/bin/glnxa64:${MCR_ROOT}/sys/os/glnxa64:${MCR_ROOT}/sys/opengl/lib/glnxa64:$LD_LIBRARY_PATH
export XAPPLRESDIR=${MCR_ROOT}/X11/app-defaults
export MCR_CACHE_ROOT=/tmp/mcr_cache

mkdir -p $MCR_CACHE_ROOT

# Запуск основной команды
exec "$@" 2>&1 | tee -a /app/logs/consumer.log
