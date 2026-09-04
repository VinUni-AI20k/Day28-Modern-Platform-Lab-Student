FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm

# uv ships as a static binary, so copy it from the published image instead of
# installing it with pip. The pip route fails on this base image and takes the
# whole devcontainer build down with it, which drops Codespaces into a recovery
# container that has no Docker and cannot run the lab stack.
COPY --from=ghcr.io/astral-sh/uv:0.12.9 /uv /uvx /usr/local/bin/
