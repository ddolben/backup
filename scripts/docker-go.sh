#!/usr/bin/env bash

image_name="golang:1.20"
container_name="go-1.20"

do_create=true
do_attach=true
do_stop=

docker_args=

while [ "$#" -gt "0" ]; do
  case "$1" in
    "--create")
      do_create=true
      shift 1
      ;;
    "--attach")
      do_attach=true
      shift 1
      ;;
    "--stop")
      do_stop=true
      shift 1
      ;;
    *)
      break
      ;;
  esac
done

args="$@"

if [ ! -z "$(docker container ls --filter name=${container_name} -q)" ]; then
  do_create=
fi

if [ ! -z "$do_stop" ]; then
  docker stop "$container_name"
  exit 0
fi

if [ ! -z "$do_create" ]; then
  echo "> Starting container"
  docker run \
    --rm \
    -it \
    -d \
    --name "$container_name" \
    -v ${PWD}:/code \
    -w /code \
    $docker_args \
    "$image_name"
fi

if [ ! -z "$do_attach" ]; then
  echo "> Attaching to container"
  if [ -z "$args" ]; then
    args=bash
  fi
  docker exec -it "$container_name" $args
fi

